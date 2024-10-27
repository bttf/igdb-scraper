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
const fetchGames = async () => {
  const res = await fetch("https://api.igdb.com/v4/games/", {
    method: "POST",
    headers: {
      "Client-ID": process.env.TWITCH_CLIENT_ID,
      Authorization: `Bearer ${_accessToken}`,
    },
    body: `fields *; sort id asc; where cover != null & id > ${_lastId}; limit ${_limit};`,
  });
  const json = await res.json();
  return json;
};

const fetchCovers = async (games) => {
  const res = await fetch("https://api.igdb.com/v4/covers/", {
    method: "POST",
    headers: {
      "Client-ID": process.env.TWITCH_CLIENT_ID,
      Authorization: `Bearer ${_accessToken}`,
    },
    body: `fields *; where id = (${games.map((g) => g.cover).join(",")});`,
  });
  const covers = await res.json();
  const urls = covers.map((c) => ({
    game: c.game,
    url: `https:${c.url.replace("t_thumb", "t_cover_big_2x")}`,
  }));
  return urls;
};

let _accessToken;
let _expiresAt;
let _retries = 0;
const run = async () => {
  try {
    _lastId = parseInt(fs.readFileSync("last_processed_id"));

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

    console.log("fetching games...");
    const games = await fetchGames();

    if (!games.length) {
      console.log("ran outta games");
      throw "exit";
    }

    // console.log("games fetched; writing to disk");
    // fs.writeFileSync("output.json", JSON.stringify(games, null, 2));

    console.log("fetching covers...");
    const covers = await fetchCovers(games);

    // fs.writeFileSync("covers.json", JSON.stringify(covers, null, 2));

    // upload images
    console.log("uploading game covers to storage bucket");
    const uploadedCovers = (
      await Promise.all(
        covers.map(async ({ game, url }) => {
          const file = await fetch(url);
          const filename = url.split("/").slice(-1)[0];
          const { data: upload, error } = await supabase.storage
            .from("game-covers")
            .upload(filename, file.body, {
              duplex: "half",
              upsert: true,
            });
          if (error) console.log("an error %s", error);
          const {
            data: { publicUrl },
          } = supabase.storage.from("game-covers").getPublicUrl(upload.path);
          return { game, url: publicUrl };
        })
      )
    ).reduce((acc, cover) => {
      return {
        ...acc,
        [cover.game]: cover.url,
      };
    }, {});

    // fs.writeFileSync("uploaded.json", JSON.stringify(uploadedCovers, null, 2));

    // upsert games
    console.log("upserting games to db");
    const { error } = await supabase
      .from("games")
      .upsert(
        games.map((g) => ({
          igdb_id: g.id,
          name: g.name,
          slug: g.slug,
          storyline: g.storyline,
          summary: g.summary,
          igdb_url: g.url,
          igdb_platform_ids: g.platforms,
          igdb_raw_payload: g,
          cover_url: uploadedCovers[g.id],
        })),
        { onConflict: "igdb_id", ignoreDuplicates: false }
      )
      .select();
    if (error) {
      console.log("there was an error upserting %s", error);
      throw error;
    } else {
      // update last id
      fs.writeFileSync("last_processed_id", `${games.slice(-1)[0].id}`);
      console.log(
        "last processed id %s; scheduling run",
        games.slice(-1)[0].id
      );
      _retries = 0;
      setTimeout(run, 250);
    }
  } catch (e) {
    if (_retries > 3) {
      console.log("retried 3 times - exiting");
    } else {
      _retries++;
      console.log("crashed - retrying %s of 3 times");
      setTimeout(run, 250);
    }
  }
};

run();
