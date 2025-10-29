import fs from "fs";
import { dirname } from "path";
import { fileURLToPath } from "url";
import mkdirp from "mkdirp";

export const __dirname = dirname(fileURLToPath(import.meta.url));

export function ensureDirs() {
  const base = `${process.cwd()}/data`;
  ["bronze", "silver", "gold"].forEach((d) => mkdirp.sync(`${base}/${d}`));
}

export async function saveRaw(bufferOrStream, destPath) {
  await mkdirp(dirname(destPath));
  const out = fs.createWriteStream(destPath);
  if (bufferOrStream.pipe) {
    bufferOrStream.pipe(out);
    await new Promise((res) => out.on("finish", res));
  } else {
    fs.writeFileSync(destPath, bufferOrStream);
  }
}
