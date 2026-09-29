import { createClient } from "./utils.js";

// Counts the sockets a client opens when requests arrive before it has
// connected. This is a count rather than a timing, so it isn't a tinybench
// task: repeating it would also pile up extra sockets on the server with
// clients that open one per request.
const REQUESTS = 50;

const monitor = createClient();
async function totalConnections(): Promise<number> {
	const stats = await monitor.stats();
	return Number(stats.get(monitor.nodes[0].id)?.total_connections);
}

const before = await totalConnections();
const client = createClient();
await Promise.all(
	Array.from({ length: REQUESTS }, (_, i) => client.get(`bench:cold:${i}`)),
);
const opened = (await totalConnections()) - before;

console.log("");
console.log("## Cold Start");
console.log("|  concurrent first requests  |  sockets opened  |");
console.log("|-----------------------------|-----------------:|");
console.log(`|  ${REQUESTS}  |  ${opened}  |`);
console.log("");

// Extra sockets from a cold start would keep the process alive.
process.exit(0);
