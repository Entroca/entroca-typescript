import { Socket } from "net";
import xxhash from "xxhash-wasm";

type Config = {
	url: string, 
	port: number, 
	threads: number,
};

type Client = {
	put: (key: Uint8Array, value: Uint8Array, ttl: number) => Promise<void>;
	get: (key: Uint8Array) => Promise<Uint8Array>;
	del: (key: Uint8Array) => Promise<void>;
	safePut: (key: Uint8Array, value: Uint8Array, ttl: number) => Promise<void>;
	safeGet: (key: Uint8Array) => Promise<Uint8Array | null>;
	safeDel: (key: Uint8Array) => Promise<void>;
};

type Queue = ((value: ArrayBufferLike) => void)[];

type Queues = Queue[];

export const createClient = async (config: Config): Promise<Client> => {
	const thread_instances: Socket[] = new Array(config.threads);
	const thread_queues: Queues = new Array(config.threads);
	const xxhash_api = await xxhash();
	const xxhash_seed = BigInt(0);
	const getHash = (input: Uint8Array) => xxhash_api.h64Raw(input, xxhash_seed); 
	const threads_bigint = BigInt(config.threads);

	for (let i = 0; i < config.threads; ++i) {
		const socket = thread_instances[i] = new Socket();	
		const queue = thread_queues[i] = [] as Queue;

		socket.on("data", (buffer)  => {
			console.log("data", buffer);

			const handler = queue.shift();
			handler?.(buffer.buffer);
		});	

		await connect(socket, config, i);
	}

	const put = async (key: Uint8Array, value: Uint8Array, ttl: number): Promise<void> => {
		const hash = getHash(key);
		const index = Number(hash % threads_bigint);
		const key_length = key.byteLength;
		const value_length = value.byteLength;
		
		return await new Promise<void>((resolve, reject) => {
			const buffer = new Uint8Array(1 + 4 + 4 + 4 + key_length + value_length);
			const view = new DataView(buffer.buffer);

			buffer[0] = 0;
			view.setUint32(1, ttl, true);
			view.setUint32(5, key_length, true);
			buffer.set(key, 9);
			view.setUint32(13 + key_length, value_length, true);
			buffer.set(value, 13 + key_length);

			console.log("[INFO] Put out", buffer);

			thread_instances[index].write(buffer, (err) => err ? reject(): resolve());
		});
	};

	const get = async (key: Uint8Array): Promise<Uint8Array> => {
		const hash = getHash(key);
		const index = Number(hash % threads_bigint);
		const key_length = key.byteLength;

		return await new Promise<Uint8Array>((resolve, reject) => {
			const timeout = setTimeout(() => {
				reject();
				clearTimeout(timeout);
			}, 30_000);

			thread_queues[index].push((value) => {
				clearTimeout(timeout);
				resolve(new Uint8Array(value));
			});

			const buffer = new Uint8Array(1 + 4 + key_length);
			const view = new DataView(buffer.buffer);

			buffer[0] = 1;
			view.setUint32(1, key_length, true);
			buffer.set(key, 5);

			console.log("[INFO] Get out", buffer);

			thread_instances[index].write(buffer, (err) => err && reject());
		});
	};

	const del = async (key: Uint8Array) => {
		const hash = getHash(key);
		const index = Number(hash % threads_bigint);
		const key_length = key.byteLength;
		
		return await new Promise<void>((resolve, reject) => {
			const buffer = new Uint8Array(1 + key_length);
			const view = new DataView(buffer.buffer);

			buffer[0] = 0;
			view.setUint32(1, key_length, true);
			buffer.set(key, 5);

			console.log("[INFO] Get out", buffer);

			thread_instances[index].write(buffer, (err) => err ? reject(): resolve());
		});
	};

	return {
		put,
		get,
		del,
		safePut: async (key: Uint8Array, value: Uint8Array, ttl: number) => {
			try {
				return await put(key, value, ttl);	
			} catch {
				return;
			}
		},
		safeGet: async (key: Uint8Array) => {
			try {
				return await get(key);	
			} catch {
				return null;
			}
		},
		safeDel: async (key: Uint8Array) => {
			try {
				return await del(key);	
			} catch {
				return;
			}
		},
	};
};

const connect = (socket: Socket, config: Config, index: number) => new Promise<void>((resolve, reject) => {
	socket.once("connect", () => {
		console.log(`[INFO] Succesfull connection ${config.url}:${config.port + index}`);
		resolve();
	});

	socket.once("error", () => {
		console.log(`[INFO] Unsuccesfull connection ${config.url}:${config.port + index}`);
		socket.destroy();
		reject();
	});

	console.log(`[INFO] Connecting to ${config.url}:${config.port + index}`);
	socket.connect(config.port + index, config.url);
});

// ---

const TEXT_ENCODER = new TextEncoder();

const key = "Hello";
const value = "World";
const ttl = 1;

const key_bytes = TEXT_ENCODER.encode(key);
const value_bytes = TEXT_ENCODER.encode(value);

const client = await createClient({
	url: "127.0.0.1",
	port: 3000,
	threads: 1,
});

console.log("put", await client.safePut(key_bytes, value_bytes, ttl));
console.log("get", await client.safeGet(key_bytes));
// console.log("del", await client.safeDel(key_bytes));
// console.log("get", await client.safeGet(key_bytes));
// console.log("put", await client.safePut(key_bytes, value_bytes, ttl));

// console.log("[INFO] Waiting 2s");
// await new Promise<void>((resolve) => setTimeout(resolve, 2000));

// console.log("get", await client.safeGet(key_bytes));
