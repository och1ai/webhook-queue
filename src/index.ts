export interface Env {
  QUEUE: DurableObjectNamespace;
  WEBHOOK_SECRET: string;
}

// Worker principal: recibe jobs y los delega al DO
export default {
  async fetch(req: Request, env: Env): Promise<Response> {
    const url = new URL(req.url);

    if (url.pathname === "/enqueue" && req.method === "POST") {
      const body = await req.json<{
        webhookUrl: string;
        payload: unknown;
        queueName?: string;
      }>();

      // Un DO por "queueName" → permite múltiples colas lógicas
      const id = env.QUEUE.idFromName(body.queueName ?? "default");
      const stub = env.QUEUE.get(id);

      return stub.fetch("https://do/enqueue", {
        method: "POST",
        body: JSON.stringify(body),
      });
    }

    return new Response("Not found", { status: 404 });
  },
};

// Durable Object que implementa la cola
export class WebhookQueue {
  state: DurableObjectState;
  env: Env;

  constructor(state: DurableObjectState, env: Env) {
    this.state = state;
    this.env = env;
  }

  async fetch(req: Request): Promise<Response> {
    const url = new URL(req.url);

    if (url.pathname === "/enqueue") {
      const job = await req.json<{
        webhookUrl: string;
        payload: unknown;
      }>();

      const jobId = crypto.randomUUID();
      const record = {
        id: jobId,
        webhookUrl: job.webhookUrl,
        payload: job.payload,
        attempts: 0,
        createdAt: Date.now(),
      };

      // Guarda el job con prefijo "job:" para listarlo después
      await this.state.storage.put(`job:${jobId}`, record);

      // Programa procesamiento inmediato si no hay alarma activa
      const currentAlarm = await this.state.storage.getAlarm();
      if (currentAlarm === null) {
        await this.state.storage.setAlarm(Date.now() + 100);
      }

      return Response.json({ jobId, status: "queued" });
    }

    return new Response("Not found", { status: 404 });
  }

  // Se dispara automáticamente cuando vence la alarma → sin polling
  async alarm() {
    const jobs = await this.state.storage.list<JobRecord>({
      prefix: "job:",
      limit: 10, // procesa en lotes pequeños
    });

    for (const [key, job] of jobs) {
      try {
        const res = await fetch(job.webhookUrl, {
          method: "POST",
          headers: {
            "Content-Type": "application/json",
            "X-Webhook-Signature": await this.sign(job.payload),
            "X-Job-Id": job.id,
          },
          body: JSON.stringify(job.payload),
        });

        if (res.ok) {
          // ACK: borra el job
          await this.state.storage.delete(key);
        } else {
          await this.handleFailure(key, job);
        }
      } catch (err) {
        await this.handleFailure(key, job);
      }
    }

    // Si quedan jobs, reprograma alarma
    const remaining = await this.state.storage.list({
      prefix: "job:",
      limit: 1,
    });
    if (remaining.size > 0) {
      await this.state.storage.setAlarm(Date.now() + 1000);
    }
  }

  async handleFailure(key: string, job: JobRecord) {
    job.attempts++;

    if (job.attempts >= 5) {
      // Mueve a dead-letter después de 5 intentos
      await this.state.storage.delete(key);
      await this.state.storage.put(`dead:${job.id}`, job);
      return;
    }

    // Backoff exponencial: 2s, 4s, 8s, 16s, 32s
    const delay = Math.pow(2, job.attempts) * 1000;
    await this.state.storage.put(key, job);
    await this.state.storage.setAlarm(Date.now() + delay);
  }

  async sign(payload: unknown): Promise<string> {
    const encoder = new TextEncoder();
    const key = await crypto.subtle.importKey(
      "raw",
      encoder.encode(this.env.WEBHOOK_SECRET),
      { name: "HMAC", hash: "SHA-256" },
      false,
      ["sign"]
    );
    const sig = await crypto.subtle.sign(
      "HMAC",
      key,
      encoder.encode(JSON.stringify(payload))
    );
    return btoa(String.fromCharCode(...new Uint8Array(sig)));
  }
}

interface JobRecord {
  id: string;
  webhookUrl: string;
  payload: unknown;
  attempts: number;
  createdAt: number;
}
