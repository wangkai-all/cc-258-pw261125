'use strict';

const { startSourceA, startSourceB } = require('./start-lib');

async function main() {
  const which = process.argv[2] || 'both';
  const basePort = Number(process.env.SOURCE_PORT || 4100);
  const started = {};
  if (which === 'a' || which === 'both') {
    const a = await startSourceA({ port: basePort });
    started.A = a;
    console.log(`source A (snapshot)  http://127.0.0.1:${a.port}`);
  }
  if (which === 'b' || which === 'both') {
    const b = await startSourceB({ port: basePort + 1 });
    started.B = b;
    console.log(`source B (revision)  http://127.0.0.1:${b.port}`);
  }
  const shutdown = () => {
    for (const source of Object.values(started)) source.close();
    process.exit(0);
  };
  process.on('SIGINT', shutdown);
  process.on('SIGTERM', shutdown);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
