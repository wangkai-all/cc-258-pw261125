'use strict';

const path = require('node:path');
const { startSourceA, startSourceB } = require('../src/sources/start-lib');
const { createQueryServer } = require('../src/coordinator/server');

async function main() {
  const basePort = Number(process.env.SOURCE_PORT || 4100);
  const port = Number(process.env.PORT || 8080);
  const dataDir = process.env.DATA_DIR || path.join(__dirname, '..', 'data');

  const a = await startSourceA({ port: basePort, dataDir });
  const b = await startSourceB({ port: basePort + 1, dataDir });
  const query = await createQueryServer({
    dbPath: path.join(dataDir, 'coordinator.sqlite'),
    sourceAUrl: `http://127.0.0.1:${a.port}`,
    sourceBUrl: `http://127.0.0.1:${b.port}`,
  });
  const actualPort = await query.listen(port);

  console.log('');
  console.log('  查询页（两个来源、三种重试口径）');
  console.log('  ------------------------------------------------');
  console.log(`  页面          http://127.0.0.1:${actualPort}/`);
  console.log(`  Source A 快照  http://127.0.0.1:${a.port}`);
  console.log(`  Source B 修订  http://127.0.0.1:${b.port}`);
  console.log(`  数据目录       ${path.resolve(dataDir)}`);
  console.log('');
  console.log('  Ctrl+C 退出。状态保存在 SQLite，重启后运行可恢复。');
  console.log('');

  const shutdown = () => {
    query.close();
    a.close();
    b.close();
    process.exit(0);
  };
  process.on('SIGINT', shutdown);
  process.on('SIGTERM', shutdown);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
