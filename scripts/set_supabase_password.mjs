#!/usr/bin/env node

import fs from "node:fs";
import path from "node:path";
import readline from "node:readline/promises";
import { fileURLToPath } from "node:url";
import { stdin as input, stdout as output } from "node:process";

function loadDotEnv(filePath) {
  if (!fs.existsSync(filePath)) {
    return;
  }
  const raw = fs.readFileSync(filePath, "utf8");
  for (const line of raw.split(/\r?\n/)) {
    const trimmed = line.trim();
    if (!trimmed || trimmed.startsWith("#")) {
      continue;
    }
    const equalsIndex = trimmed.indexOf("=");
    if (equalsIndex <= 0) {
      continue;
    }
    const key = trimmed.slice(0, equalsIndex).trim();
    let value = trimmed.slice(equalsIndex + 1).trim();
    if (
      (value.startsWith('"') && value.endsWith('"')) ||
      (value.startsWith("'") && value.endsWith("'"))
    ) {
      value = value.slice(1, -1);
    }
    if (!(key in process.env)) {
      process.env[key] = value;
    }
  }
}

const scriptDir = path.dirname(fileURLToPath(import.meta.url));
const repoRoot = path.resolve(scriptDir, "..");
loadDotEnv(path.join(repoRoot, ".env"));

const [, , emailArg, passwordArg, displayNameArg] = process.argv;

const supabaseUrl = (process.env.VITE_SUPABASE_URL || "").trim();
const serviceRoleKey = (process.env.SUPABASE_SERVICE_ROLE_KEY || "").trim();

if (!supabaseUrl) {
  console.error("Missing VITE_SUPABASE_URL in environment or .env");
  process.exit(1);
}

if (!serviceRoleKey) {
  console.error("Missing SUPABASE_SERVICE_ROLE_KEY in environment or .env");
  console.error("Get it from Supabase Dashboard -> Project Settings -> API -> service_role");
  process.exit(1);
}

const headers = {
  apikey: serviceRoleKey,
  Authorization: `Bearer ${serviceRoleKey}`,
  "Content-Type": "application/json",
};

async function request(url, options = {}) {
  const response = await fetch(url, {
    ...options,
    headers: {
      ...headers,
      ...(options.headers || {}),
    },
  });
  const text = await response.text();
  let body = null;
  try {
    body = text ? JSON.parse(text) : null;
  } catch {
    body = text;
  }
  if (!response.ok) {
    const detail =
      typeof body === "string" ? body : JSON.stringify(body, null, 2);
    throw new Error(`HTTP ${response.status} ${response.statusText}: ${detail}`);
  }
  return body;
}

async function findUserByEmail(email) {
  let page = 1;
  for (;;) {
    const payload = await request(
      `${supabaseUrl}/auth/v1/admin/users?page=${page}&per_page=1000`,
      { method: "GET" }
    );
    const users = Array.isArray(payload?.users) ? payload.users : [];
    const match = users.find(
      (user) => String(user?.email || "").toLowerCase() === email.toLowerCase()
    );
    if (match) {
      return match;
    }
    if (users.length < 1000) {
      return null;
    }
    page += 1;
  }
}

function buildUserPayload(password, displayName) {
  const payload = {
    password,
    email_confirm: true,
  };
  if (displayName) {
    payload.user_metadata = {
      display_name: displayName,
      full_name: displayName,
    };
  }
  return payload;
}

async function createUser(email, password, displayName) {
  return request(`${supabaseUrl}/auth/v1/admin/users`, {
    method: "POST",
    body: JSON.stringify({
      email,
      ...buildUserPayload(password, displayName),
    }),
  });
}

async function updateUser(userId, password, displayName) {
  return request(`${supabaseUrl}/auth/v1/admin/users/${encodeURIComponent(userId)}`, {
    method: "PUT",
    body: JSON.stringify(buildUserPayload(password, displayName)),
  });
}

async function main() {
  let email = emailArg;
  let password = passwordArg;
  let displayName = displayNameArg;

  if (!email || !password) {
    const rl = readline.createInterface({ input, output });
    try {
      if (!email) {
        email = (await rl.question("Email: ")).trim();
      }
      const existingUserPreview = email ? await findUserByEmail(email) : null;
      if (existingUserPreview) {
        console.log(`Found existing user: ${email}`);
        console.log(`user_id=${existingUserPreview.id}`);
        const nextDisplayName =
          existingUserPreview.user_metadata?.display_name ||
          existingUserPreview.user_metadata?.full_name ||
          "";
        if (!displayName && nextDisplayName) {
          console.log(`Current display name: ${nextDisplayName}`);
        }
      } else {
        console.log(`No existing user found for: ${email}`);
        console.log("A new user will be created.");
      }
      if (!password) {
        password = (await rl.question("New password: ")).trim();
      }
      if (!displayName) {
        displayName = (await rl.question("Display name (optional): ")).trim();
      }
      const action = existingUserPreview ? "update" : "create";
      const confirmation = (await rl.question(`Proceed to ${action} user? (y/N): `))
        .trim()
        .toLowerCase();
      if (confirmation !== "y" && confirmation !== "yes") {
        console.log("Cancelled.");
        return;
      }
    } finally {
      rl.close();
    }
  }

  if (!email || !password) {
    console.error("Email and password are required.");
    process.exit(1);
  }

  const existingUser = await findUserByEmail(email);
  if (!existingUser) {
    const created = await createUser(email, password, displayName);
    console.log(`Created user ${email}`);
    console.log(`user_id=${created?.id || created?.user?.id || ""}`);
    if (displayName) {
      console.log(`display_name=${displayName}`);
    }
    return;
  }

  await updateUser(existingUser.id, password, displayName);
  console.log(`Updated password for ${email}`);
  console.log(`user_id=${existingUser.id}`);
  if (displayName) {
    console.log(`display_name=${displayName}`);
  }
}

main().catch((error) => {
  console.error(error instanceof Error ? error.message : String(error));
  process.exit(1);
});
