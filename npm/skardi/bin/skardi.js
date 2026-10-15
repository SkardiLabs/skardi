#!/usr/bin/env node

// Resolves the platform-specific @skardi/cli-* package (pulled in via
// optionalDependencies, so npm only installs the one matching this machine)
// and runs the real skardi binary, forwarding arguments, stdio, exit codes
// and signals.

const { spawnSync } = require("child_process");
const path = require("path");

const PLATFORM_PACKAGES = {
  "darwin arm64": "@skardi/cli-darwin-arm64",
  "linux arm64": "@skardi/cli-linux-arm64",
  "linux x64": "@skardi/cli-linux-x64",
};

function fail(message) {
  console.error(`skardi: ${message}`);
  process.exit(1);
}

const pkg = PLATFORM_PACKAGES[`${process.platform} ${process.arch}`];
if (!pkg) {
  fail(
    `no prebuilt binary for ${process.platform}/${process.arch}. ` +
      "Supported platforms: macOS arm64, Linux x86_64, Linux arm64. " +
      "Other install options: https://github.com/SkardiLabs/skardi#install"
  );
}

let packageDir;
try {
  packageDir = path.dirname(require.resolve(`${pkg}/package.json`));
} catch {
  fail(
    `the ${pkg} package is not installed. If you installed with --no-optional, ` +
      "reinstall without it; otherwise this is a packaging bug — please report " +
      "it at https://github.com/SkardiLabs/skardi/issues."
  );
}

const result = spawnSync(path.join(packageDir, "bin", "skardi"), process.argv.slice(2), {
  stdio: "inherit",
});

if (result.error) {
  fail(`failed to launch the skardi binary: ${result.error.message}`);
}
if (result.signal) {
  // Die the same way so parent processes observe the real termination reason.
  process.kill(process.pid, result.signal);
} else {
  process.exit(result.status === null ? 1 : result.status);
}
