import fs from "node:fs";
import path from "node:path";

const packageJsonPath = path.resolve(process.cwd(), "package.json");
const packageJson = JSON.parse(fs.readFileSync(packageJsonPath, "utf8"));

const dependencyGroups = [
  ["dependencies", packageJson.dependencies || {}],
  ["devDependencies", packageJson.devDependencies || {}],
  ["optionalDependencies", packageJson.optionalDependencies || {}],
];

for (const [groupName, deps] of dependencyGroups) {
  for (const [name, spec] of Object.entries(deps)) {
    const cleanSpec = String(spec || "").trim();
    if (name === "abs-mcp-server" || cleanSpec === "file:.." || cleanSpec.startsWith("file:../")) {
      console.error(
        `Forbidden frontend dependency detected in ${groupName}: ${name}=${cleanSpec}. ` +
          "Do not depend on the repo root package from frontend/package.json."
      );
      process.exit(1);
    }
  }
}
