import Redis from 'ioredis';
import { createPool } from 'generic-pool';
import pino from 'pino';

const logger = pino({ level: 'debug' });

export default class Queue {

    processMap = new Map();
    isReady = false; // 🔥 Nueva bandera para asegurarse de que subscribe() esté listo antes de publicar.

    constructor(credentials) {
        this.credentials = credentials;
        this.client = new Redis(credentials);
        this.client.on('connect', () => logger.debug('Conexión a Redis establecida'));
        this.client.on('error', (err) => logger.error(`Error en Redis: ${err.message}`));

        this.pool = createPool({
            create: async () => {
                const client = new Redis(credentials);
                client.on('error', (err) => logger.error(`Error en conexión del pool: ${err.message}`));
                return client;
            },
            destroy: async (client) => {
                logger.debug('Cerrando cliente de Redis del pool');
                await client.quit();
            },
            validate: async (client) => {
                try {
                    await client.ping();
                    return true;
                } catch {
                    return false;
                }
            }
        }, { max: 10, min: 2 });

        logger.debug('Pool de conexiones creado');

        this.subscriber = new Redis(this.credentials);
        this.listenToPubSub();
    }

    async getClient() {
        logger.debug('Adquiriendo cliente de Redis');
        return this.pool.acquire();
    }

    async releaseClient(client) {
        logger.debug('Liberando cliente de Redis');
        await this.pool.release(client);
    }

    async listenToPubSub() {
        logger.info("📡 Suscribiéndose al canal 'QUEUE-NOTIFY' en Redis Pub/Sub...");

        await new Promise((resolve, reject) => {
            this.subscriber.subscribe('QUEUE-NOTIFY', (err, count) => {
                if (err) {
                    logger.error(`❌ Error suscribiéndose a 'QUEUE-NOTIFY': ${err.message}`);
                    reject(err);
                } else {
                    logger.info(`✅ Suscrito a ${count} canal(es), esperando mensajes...`);
                    this.isReady = true; // 🔥 Marcamos que el suscriptor ya está listo
                    resolve();
                }
            });
        });

        this.subscriber.on('message', (channel, message) => {
            if (channel === 'QUEUE-NOTIFY') {
                try {
                    const { task, group } = JSON.parse(message);
                    logger.info(`🔔 Notificación recibida: Nuevo mensaje en task '${task}', grupo '${group}'`);

                    if (this.processMap.has(task)) {
                        logger.info(`✅ Se encontró callback para el task '${task}', ejecutando...`);
                        this.processMap.get(task)({ task, group });
                    } else {
                        logger.warn(`⚠️ No hay callback asociado al task '${task}', ignorando notificación.`);
                    }
                } catch (error) {
                    logger.error("❌ Error procesando mensaje de Pub/Sub:", error);
                }
            }
        });
    }

    async add(task, group, data) {
        // 🔥 Esperar a que `subscribe()` esté listo antes de publicar
        while (!this.isReady) {
            logger.warn("⏳ Esperando a que el suscriptor de Redis esté listo...");
            await new Promise(resolve => setTimeout(resolve, 100)); // Espera 100ms y vuelve a intentar
        }

        const client = await this.getClient();
        await client.publish('QUEUE-NOTIFY', JSON.stringify({ task, group }));
        logger.info(`📢 Notificación enviada: Nuevo mensaje en task '${task}', grupo '${group}'`);

        return "xxxx";
    }

    async process(task, nConsumers = 1, callback) {
        this.processMap.set(task, callback);
    }

    async close() {
        logger.debug('Cerrando conexiones de Redis y pool');
        await this.pool.drain();
        await this.pool.clear();
        await this.client.quit();
    }
}