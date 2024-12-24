require("dotenv").config();
const fs = require("fs");
const { createClient } = require("@supabase/supabase-js");

const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SERVICE_KEY
);

const authenticate = async () => {
  const res = await fetch(
    `https://id.twitch.tv/oauth2/token?client_id=${process.env.TWITCH_CLIENT_ID}&client_secret=${process.env.TWITCH_CLIENT_SECRET}&grant_type=client_credentials`,
    {
      method: "POST",
    }
  );
  const json = await res.json();
  return json;
};

let _limit = 4; // how many to process at a time
let _lastId = -1;
const fetchPlatforms = async () => {
  let res = await fetch("https://api.igdb.com/v4/platforms", {
    method: "POST",
    headers: {
      "Client-ID": process.env.TWITCH_CLIENT_ID,
      Authorization: `Bearer ${_accessToken}`,
    },
    body: `fields *; sort id asc; where id > ${_lastId}; limit ${_limit};`,
  });
  const platforms = await res.json();
  return platforms
    .map((p) => ({
      ...p,
      category: getCategory(p.category),
    }))
    .filter((p) => p.category);
};

const getCategory = (id) => {
  switch (id) {
    case 1:
      return "console";
    case 2:
      return "arcade";
    case 3:
      return "platform";
    case 4:
      return "operating_system";
    case 5:
      return "portable_console";
    case 6:
      return "computer";
  }
};

let _accessToken;
let _expiresAt;
let _retries = 0;
const run = async () => {
  try {
    _lastId = parseInt(fs.readFileSync("last_processed_platform_id"));

    if (!_accessToken || new Date() > _expiresAt) {
      console.log("authenticating ...");
      const creds = await authenticate();
      _accessToken = creds.access_token;
      _expiresAt = new Date(
        new Date().setSeconds(new Date().getSeconds() + creds.expires_in)
      );
      console.log("authenticated", {
        _accessToken,
        _expiresAt: _expiresAt.toISOString(),
      });
    }

    console.log("fetching platforms...");
    const platforms = await fetchPlatforms();

    if (!platforms.length) {
      console.log("ran outta platforms");
      throw "exit";
    }

    console.log("upserting platforms to db");
    const { error } = await supabase
      .from("platforms")
      .upsert(
        platforms.map((p) => ({
          slug: p.slug,
          name: p.name,
          category: p.category,
          igdb_id: p.id,
          igdb_payload: p,
        })),
        { onConflict: "igdb_id", ignoreDuplicates: false }
      )
      .select();

    if (error) {
      console.log("there was an error upserting %s", error);
      throw error;
    } else {
      // update last id
      fs.writeFileSync(
        "last_processed_platform_id",
        `${platforms.slice(-1)[0].id}`
      );
      console.log(
        "last processed id %s; scheduling run",
        platforms.slice(-1)[0].id
      );
      _retries = 0;
      setTimeout(run, 250);
    }
  } catch (e) {
    console.log("encountered error", e);
    if (_retries > 3) {
      console.log("retried 3 times - exiting");
    } else {
      _retries++;
      console.log("crashed - retrying %s of 3 times", _retries);
      setTimeout(run, 250);
    }
  }
};

run();
