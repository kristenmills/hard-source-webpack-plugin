const fs = require('fs');
const {join, resolve} = require('path');
const _mkdirp = require('mkdirp');

const promisify = fn => (...args) => {
  return new Promise((resolve, reject) => {
    args.push((err, value) => {
      if (err) {
        return reject(err);
      }
      return resolve(value);
    });
    fn(...args);
  });
};

const read = promisify(fs.readFile);
const readdir = promisify(fs.readdir);
const write = promisify(fs.writeFile);
const mkdirp = promisify(_mkdirp);

const open = promisify(fs.open);
const close = promisify(fs.close);
const readfd = promisify(fs.read);

let tmpBuffer = new Buffer(0.5 * 1024 * 1024);
let outBuffer = new Buffer(2.5 * 1024 * 1024);

class WriteOutput {
  constructor() {
    this.length = 0;
    this.table = [];
    this.buffer = outBuffer;
  }

  add(key, content) {
    let length = tmpBuffer.utf8Write(content);
    while (length === tmpBuffer.length) {
      const tmpBuffer2 = Buffer.allocUnsafe(tmpBuffer.length * 2);
      tmpBuffer.copy(tmpBuffer2);
      tmpBuffer = tmpBuffer2;
      length = tmpBuffer.utf8Write(content);
    }

    const start = this.length;
    const end = start + length;

    if (end > this.buffer.length) {
      const endPow2 = Math.pow(2, Math.ceil(Math.log(end) / Math.log(2)));
      const tmpBuffer2 = Buffer.allocUnsafe(endPow2);
      this.buffer.copy(tmpBuffer2);
      this.buffer = outBuffer = tmpBuffer2;
    }

    tmpBuffer.copy(this.buffer.slice(start, end));

    this.table.push({
      name: key,
      start,
      end,
    });
    this.length = end;
  }
}

class Semaphore {
  constructor(max) {
    this.max = max;
    this.count = 0;
    this.next = [];
  }

  async guard() {
    if (this.count < this.max) {
      this.count++;
      return new SemaphoreGuard(this);
    }
    else {
      return new Promise(resolve => {
        this.next.push(resolve);
      })
      .then(() => new SemaphoreGuard(this));
    }
  }
}

class SemaphoreGuard {
  constructor(parent) {
    this.parent = parent;
  }

  done() {
    const next = this.parent.next.shift();
    if (next) {
      next();
    }
    else {
      this.parent.count--;
    }
  }
}

class Append2 {
  constructor({cacheDirPath: path}) {
    this.path = path;
    this.inBuffer = new Buffer(0);
    this._buffers = [];
    this.outBuffer = new Buffer(0);
  }

  async read() {
    const out = {};
    const order = {};

    await mkdirp(this.path);

    const items = await readdir(this.path);
    const logs = items.filter(item => /^log\d+$/.test(item));
    logs.sort();
    const reverseLogs = logs.reverse();

    const sema = new Semaphore(16);
    return Promise.all(reverseLogs.map(async (file, index) => {
      file = join(this.path, file);
      const guard = await sema.guard();
      const fd = await open(file, 'r+');

      let body = this._buffers.pop() || Buffer.allocUnsafe(2.5 * 1024 * 1024);
      await readfd(fd, body, 0, 4, 0);
      const fullLength = body.readUInt32LE(0);
      if (fullLength > body.length) {
        this._buffers.push(body);
        body = null;
        const bodyIndex = this._buffers.findIndex(buf => buf.length >= fullLength);
        if (bodyIndex > -1) {
          body = this._buffers[bodyIndex];
          this._buffers.splice(bodyIndex, 1);
        }
        else {
          const endPow2 = Math.pow(2, Math.ceil(Math.log(fullLength) / Math.log(2)));
          body = Buffer.allocUnsafe(endPow2);
        }
      }
      await readfd(fd, body, 0, fullLength, 4);

      close(fd);

      const tableLength = body.readUInt32LE(0);
      const table = JSON.parse(body.utf8Slice(4, 4 + tableLength));
      const content = body.slice(4 + tableLength);

      const totalSize = table.reduce((carry, entry) => entry.end - entry.start + carry, 0);

      for (const entry of table) {
        if (
          typeof order[entry.name] === 'undefined' ||
          order[entry.name] > index
        ) {
          order[entry.name] = index;

          if (entry.start !== entry.end) {
            await new Promise(process.nextTick);
            out[entry.name] = JSON.parse(
              content.utf8Slice(entry.start, entry.end)
            );
          }
          else {
            out[entry.name] = null;
          }
        }
      }

      this._buffers.push(body);
      guard.done();
    }))
    .then(() => out);
  }

  async _markLog() {
    const count = (await readdir(this.path)).filter(item => /log\d+$/.test(item)).length;
    const marker = Math.random().toString(16).substring(2).padStart(13, '0');
    const file = resolve(this.path, `log${count.toString().padStart(4, '0')}`);
    await write(file, marker);
    const writtenMarker = await read(file, 'utf8');
    if (marker === writtenMarker) {
      return file;
    }
    return null;
  }

  async _write(file, smallOutput) {
    const content = JSON.stringify(smallOutput.table);
    let length = tmpBuffer.utf8Write(content, 8);
    while (length === tmpBuffer.length) {
      const tmpBuffer2 = Buffer.allocUnsafe(tmpBuffer.length * 2);
      tmpBuffer.copy(tmpBuffer2);
      tmpBuffer = tmpBuffer2;
      length = tmpBuffer.utf8Write(content, 8);
    }

    const end = 8 + length + smallOutput.length;
    if (end > tmpBuffer.length) {
      const endPow2 = Math.pow(2, Math.ceil(Math.log(end) / Math.log(2)));
      const tmpBuffer2 = Buffer.allocUnsafe(endPow2);
      tmpBuffer.copy(tmpBuffer2);
      tmpBuffer = tmpBuffer2;
    }
    smallOutput.buffer.copy(tmpBuffer.slice(8 + length, end));

    // Full length after this uint.
    tmpBuffer.writeUInt32LE(end - 4, 0);
    // Length of table after this uint.
    tmpBuffer.writeUInt32LE(length, 4);

    await write(file, tmpBuffer.slice(0, end));

    // await Promise.all([
    //   write(`${file}-table.json`, JSON.stringify(smallOutput.table)),
    //   write(file, smallOutput.buffer.slice(0, smallOutput.length)),
    // ]);
  }

  async write(ops) {
    let smallOutput = new WriteOutput();
    let largeOutput = new WriteOutput();

    const outputPromises = [];

    await mkdirp(this.path);

    for (const op of ops) {
      if (op.value !== null) {
        const content = JSON.stringify(op.value);
        if (content.length < 64 * 1024) {
          // console.log('small');
          smallOutput.add(op.key, content);

          while (smallOutput.length >= 2 * 1024 * 1024) {
            // console.log('write small');
            const file = await this._markLog();
            if (file !== null) {
              await (this._write(file, smallOutput));
              smallOutput = new WriteOutput();
            }
          }
        }
        else {
          // console.log('larg');
          largeOutput.add(op.key, content);

          while (largeOutput.length >= 2 * 1024 * 1024) {
            // console.log('write large');
            const file = await this._markLog();
            if (file !== null) {
              await (this._write(file, largeOutput));
              largeOutput = new WriteOutput();
            }
          }
        }
      }
    }

    while (smallOutput.length > 0) {
      // console.log('write small last');
      const file = await this._markLog();
      if (file !== null) {
        await (this._write(file, smallOutput));
        smallOutput = new WriteOutput();
      }
    }

    while (largeOutput.length > 0) {
      // // console.log('write large last');
      const file = await this._markLog();
      if (file !== null) {
        await (this._write(file, largeOutput));
        largeOutput = new WriteOutput();
      }
    }

    await Promise.all(outputPromises);
  }
}

module.exports = Append2;
