import Redis from 'ioredis';
import { createPool } from 'generic-pool';
import pino from 'pino';

const logger = pino({ level: 'debug' });

export default class Queue {
    constructor(credentials) {
        this.credentials = credentials;
        this.client = new Redis({
            host: credentials.host,
            port: credentials.port,
            password: credentials.password,
            db: credentials.db,
        });

        this.client.on('connect', () => logger.debug('Conexión a Redis establecida'));
        this.client.on('error', (err) => logger.error(`Error en Redis: ${err.message}`));

        this.pool = createPool({
            create: async () => {
                const client = new Redis({
                    host: credentials.host,
                    port: credentials.port,
                    password: credentials.password,
                    db: credentials.db,
                });
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
    }

    async getClient() {
        logger.debug('Adquiriendo cliente de Redis');
        return this.pool.acquire();
    }

    async releaseClient(client) {
        logger.debug('Liberando cliente de Redis');
        await this.pool.release(client);
    }

    async close() {
        logger.debug('Cerrando conexiones de Redis y pool');
        await this.pool.drain();
        await this.pool.clear();
        await this.client.quit();
    }
}