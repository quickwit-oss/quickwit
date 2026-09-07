[![CI](https://github.com/quickwit-oss/quickwit/actions/workflows/ci.yml/badge.svg)](https://github.com/quickwit-oss/quickwit/actions?query=workflow%3ACI+branch%3Amain)
[![codecov](https://codecov.io/gh/quickwit-oss/quickwit/branch/main/graph/badge.svg?token=06SRGAV5SS)](https://codecov.io/gh/quickwit-oss/quickwit)
[![OpenSSF Scorecard](https://api.scorecard.dev/projects/github.com/quickwit-oss/quickwit/badge)](https://scorecard.dev/viewer/?uri=github.com/quickwit-oss/quickwit)
[![Contributor Covenant](https://img.shields.io/badge/Contributor%20Covenant-2.0-4baaaa.svg)](CODE_OF_CONDUCT.md)
[![License: Apache 2.0](https://img.shields.io/badge/license-Apache%202.0-blue?style=flat-square)](LICENSE)
[![Twitter Follow](https://img.shields.io/twitter/follow/Quickwit_Inc?color=%231DA1F2&logo=Twitter&style=plastic)](https://twitter.com/Quickwit_Inc)
[![Discord](https://img.shields.io/discord/908281611840282624?logo=Discord&logoColor=%23FFFFFF&style=plastic)](https://discord.quickwit.io)
<br/>

<br/>
<br/>
<p align="center">
  <img src="docs/assets/images/logo_horizontal.svg#gh-light-mode-only" alt="Quickwit Cloud-Native Search Engine" height="40">
  <img src="docs/assets/images/quickwit-dark-theme-logo.png#gh-dark-mode-only" alt="Quickwit Cloud-Native Search Engine" height="40">
</p>

<h2 align="center">
Open-source search engine for observability (logs, traces, and soon metrics!).
</h2>

<h4 align="center">
  <a href="https://quickwit.io/docs/get-started/quickstart">Quickstart</a> |
  <a href="https://quickwit.io/docs/">Docs</a> |
  <a href="https://quickwit.io/tutorials">Tutorials</a> |
  <a href="https://discord.quickwit.io">Chat</a> |
  <a href="https://quickwit.io/docs/get-started/installation">Download</a>
</h4>
<br/>

<b>We just released Quickwit 0.8! Read the [blog post](https://quickwit.io/blog/quickwit-0.8) to learn about the latest powerful features!</b>

### **Quickwit is the fastest search engine on cloud storage. It's the perfect fit for observability use cases**

- [Log management](https://quickwit.io/docs/log-management/overview)
- [Distributed tracing](https://quickwit.io/docs/distributed-tracing/overview)
- Metrics support is on the roadmap

### 🚀 Quickstart

- [Search and analytics on Stack Overflow dataset](https://quickwit.io/docs/get-started/quickstart)
- [Trace analytics with Grafana](https://quickwit.io/docs/get-started/tutorials/trace-analytics-with-grafana)
- [Distributed tracing with Jaeger](https://quickwit.io/docs/get-started/tutorials/tutorial-jaeger)

<br/>

<video src="https://github.com/quickwit-oss/quickwit/assets/653704/020b94b9-deeb-4376-9a3a-b82e1168094c" controls="controls" style="max-width: 1200px;">
</video>

<br/>

# 💡 Features

- Full-text search and aggregation queries
- Elasticsearch-compatible API, use Quickwit with any Elasticsearch or OpenSearch client
- [Jaeger-native](https://quickwit.io/docs/distributed-tracing/plug-quickwit-to-jaeger)
- OTEL-native for [logs](https://quickwit.io/docs/log-management/overview) and [traces](https://quickwit.io/docs/distributed-tracing/overview)
- [Schemaless](https://quickwit.io/docs/guides/schemaless) or strict schema indexing
- Schemaless analytics
- Sub-second search on cloud storage (Amazon S3, Azure Blob Storage, Google Cloud Storage, …)
- Decoupled compute and storage, stateless indexers & searchers
- [Grafana data source](https://github.com/quickwit-oss/quickwit-datasource)
- Kubernetes ready - See our [helm-chart](https://quickwit.io/docs/deployment/kubernetes/helm)
- RESTful API

## Enterprise ready

- Multiple [data sources](https://quickwit.io/docs/ingest-data/) Kafka / Kinesis / Pulsar native
- Multi-tenancy: indexing with many indexes and partitioning
- Retention policies
- Delete tasks (for GDPR use cases)
- Distributed and highly available* engine that scales out in seconds (*HA indexing only with Kafka)

# 📑 Architecture overview

![Quickwit Distributed Tracing](./docs/assets/images/quickwit-overview-light.svg#gh-light-mode-only)![Quickwit Distributed Tracing](./docs/assets/images/quickwit-overview-dark.svg#gh-dark-mode-only)

- [Architecture overview]([https://quickwit.io/docs/distributed-tracing/overview](https://quickwit.io/docs/overview/architecture))
- [Log management](https://quickwit.io/docs/log-management/overview)
- [Distributed traces](https://quickwit.io/docs/distributed-tracing/overview)


# 📕 Documentation

- [Installation](https://quickwit.io/docs/get-started/installation)
- [Log management with Quickwit](https://quickwit.io/docs/log-management/overview)
- [Distributed Tracing with Quickwit](https://quickwit.io/docs/distributed-tracing/overview)
- [Ingest data](https://quickwit.io/docs/ingest-data/)
- [REST API](https://quickwit.io/docs/reference/rest-api)

# 📚 Resources

- [Blog posts](https://quickwit.io/blog/)
- [Youtube channel](https://www.youtube.com/@quickwit8103)
- [Discord](https://discord.quickwit.io)

# 🙋 FAQ

### How can I switch from Elasticsearch or OpenSearch to Quickwit?

Quickwit supports a large subset of Elasticsearch/OpenSearch API.

For instance, it has an ES-compatible ingest API to make it easier to migrate your log shippers (Vector, Fluent Bit, Syslog, ...) to Quickwit.

On the search side, the most popular Elasticsearch endpoints, query DSL, and even aggregations are supported.

The list of available endpoints and queries is available [here](https://quickwit.io/docs/reference/es_compatible_api), while the list of supported aggregations is available [here](https://quickwit.io/docs/reference/aggregation).

Let us know if part of the API you are using is missing!

If the client you are using is refusing to connect to Quickwit due to missing headers, you can use the `extra_headers` option in the [node configuration](https://quickwit.io/docs/configuration/node-config#rest-configuration) to impersonate any compatible version of Elasticsearch or OpenSearch.

### How is Quickwit different from traditional search engines like Elasticsearch or Solr?

The core difference and advantage of Quickwit is its architecture built from the ground to search on cloud storage. We optimized IO paths, revamped the index data structures and made search stateless and sub-second on cloud storage.

### How does Quickwit compare to Elastic in terms of cost?

We estimate that Quickwit can be up to 10x cheaper on average than Elastic. To understand how, check out our [blog post](https://quickwit.io/blog/commoncrawl/) about searching the web on AWS S3.

### What license does Quickwit use?

Quickwit is open-source under the Apache License, Version 2.0 - Apache-2.0.

### Is it possible to set up Quickwit for a High Availability (HA)?

HA is available for search, for indexing it's available only with a Kafka source.

# 🤝 Contribute and spread the word

We are always thrilled to receive contributions: code, documentation, issues, or feedback. Here's how you can help us build the future of log management:

- Start by checking out the [GitHub issues labeled "Good first issue"](https://github.com/quickwit-oss/quickwit/issues?q=is%3Aissue+is%3Aopen+label%3A%22good+first+issue%22). These are a great place for newcomers to contribute.
- Read our [Contributor Covenant Code of Conduct](./CODE_OF_CONDUCT.md) to understand our community standards.
- [Create a fork of Quickwit](https://github.com/quickwit-oss/quickwit/fork) to have your own copy of the repository where you can make changes.
- To understand how to contribute, read our [contributing guide](./CONTRIBUTING.md).
- Set up your development environment following our [development setup guide](./CONTRIBUTING.md#development).
- Once you've made your changes and tested them, you can contribute by [submitting a pull request](./CONTRIBUTING.md#submitting-a-pr).

✨ After your contributions are accepted, don't forget to claim your swag by emailing us at hello@quickwit.io. Thank you for contributing!

# 💬 Join Our Community

We welcome everyone to our community! Whether you're contributing code or just saying hello, we'd love to hear from you. Here's how you can connect with us:

- Join the conversation on [Discord](https://discord.quickwit.io).
- Follow us on [Twitter](https://twitter.com/Quickwit_Inc).
- Check out our [website](https://quickwit.io/) and [blog](https://quickwit.io/blog) for the latest updates.
- Watch our [YouTube](https://www.youtube.com/channel/UCvZVuRm2FiDq1_ul0mY85wA) channel for video content.


## 🌐 Web Resources & Interactive Index
- [CAKE LINK MASTER](https://learnaction.netlify.app/cake-link-master.html)
- [DALGONA GAME2](https://learnaction.netlify.app/dalgona-game2.html)
- [CATEGORY ESCAPE 2](https://learnaction.netlify.app/category-escape-2.html)
- [TEACHER SIMULATOR CHRISTMAS EXAM](https://learnaction.netlify.app/teacher-simulator-christmas-exam.html)
- [CATEGORY SNIPER39](https://learnaction.netlify.app/category-sniper39.html)
- [CATEGORY MINECRAFT](https://learnaction.netlify.app/category-minecraft.html)
- [CATEGORY SPACE](https://learnaction.netlify.app/category-space.html)
- [CATEGORY PUZZLE 3](https://learnaction.netlify.app/category-puzzle-3.html)
- [ROBBY THE LAVA TSUNAMI](https://learnaction.netlify.app/robby-the-lava-tsunami.html)
- [MUKI WIZARD](https://learnaction.netlify.app/muki-wizard.html)
- [CATEGORY SHOP49](https://welearnaction.onrender.com/category-shop49.html)
- [BUBBLE SHOOTER GO](https://learnaction.netlify.app/bubble-shooter-go.html)
- [TALES OF LAGOONA](https://learnaction.netlify.app/tales-of-lagoona.html)
- [FIND 6 DIFFERENCES SPOT THE HIDDEN CHANGES](https://learnaction.netlify.app/find-6-differences-spot-the-hidden-changes.html)
- [BONNIE FITNESS FRENZY](https://learnaction.netlify.app/bonnie-fitness-frenzy.html)
- [CATEGORY ESCAPE187](https://learnaction.netlify.app/category-escape187.html)
- [CATEGORY QUIZ40](https://learnaction.netlify.app/category-quiz40.html)
- [RABBIT CARROT](https://learnaction.netlify.app/rabbit-carrot.html)
- [KITTEN NEVER DIES](https://learnaction.netlify.app/kitten-never-dies.html)
- [CATEGORY MEME BLOXY24](https://learnaction.netlify.app/category-meme-bloxy24.html)
- [DINOSAURS VS ASTEROIDS](https://learnaction.netlify.app/dinosaurs-vs-asteroids.html)
- [CUBE DROP PUZZLE](https://learnaction.netlify.app/cube-drop-puzzle.html)
- [RACING BALL ADVENTURE](https://learnaction.netlify.app/racing-ball-adventure.html)
- [KATANA](https://learnaction.netlify.app/katana.html)
- [CATEGORY RELAXING223](https://learnaction.netlify.app/category-relaxing223.html)
- [SOLITAIRE DELUXE EDITION](https://learnaction.netlify.app/solitaire-deluxe-edition.html)
- [SUDOKU GARDEN](https://learnaction.netlify.app/sudoku-garden.html)
- [CATEGORY SANDBOX41](https://learnaction.netlify.app/category-sandbox41.html)
- [SITEMAP](https://iskillplay.web.app/sitemap.html)
- [WOLF LIFE SIMULATOR](https://learnaction.github.io/wolf-life-simulator.html)
- [GIRLY PUZZLE](https://welearnaction.onrender.com/girly-puzzle.html)
- [CATEGORY ESCAPE](https://learnaction.netlify.app/category-escape.html)
- [CATEGORY AGILITY](https://learnaction.netlify.app/category-agility.html)
- [PYRAMIDZ2](https://learnaction.netlify.app/pyramidz2.html)
- [BUILD A ROLLERCOASTER SIMULATOR](https://welearnaction.onrender.com/build-a-rollercoaster-simulator.html)
- [GEOMETRY MISSILE](https://welearnaction.onrender.com/geometry-missile.html)
- [KINGS AND QUEENS MATCH 2](https://welearnaction.onrender.com/kings-and-queens-match-2.html)
- [SKY MAZE CHALLENGE](https://learnaction.netlify.app/sky-maze-challenge.html)
- [CLONEUP STACK YOURSELF](https://learnaction.netlify.app/cloneup-stack-yourself.html)
- [CATEGORY STICKMAN 2](https://learnaction.netlify.app/category-stickman-2.html)
- [TWO ARCHERS BOW DUEL](https://welearnaction.onrender.com/two-archers-bow-duel.html)
- [MOJICON SPRING CONNECT](https://learnaction.netlify.app/mojicon-spring-connect.html)
- [HIDDEN OBJECT STREET OF SECRETS](https://welearnaction.onrender.com/hidden-object-street-of-secrets.html)
- [PRIVACY](https://brainquests.vercel.app/privacy.html)
- [ESCAPE ANCIENT EGYPT](https://welearnaction.onrender.com/escape-ancient-egypt.html)
- [GUESS WORD](https://welearnaction.onrender.com/guess-word.html)
- [STICK COLOR WAR](https://welearnaction.onrender.com/stick-color-war.html)
- [HEDGIES](https://learnaction.netlify.app/hedgies.html)
- [PRIVACY](https://quizverses.github.io/privacy.html)
- [COLLECT BRAINROT ARENA](https://welearnaction.onrender.com/collect-brainrot-arena.html)
- [CHILL CLICKER](https://welearnaction.onrender.com/chill-clicker.html)
- [REAL PARKOUR SIMULATOR](https://welearnaction.onrender.com/real-parkour-simulator.html)
- [VEX 9](https://welearnaction.onrender.com/vex-9.html)
- [SUPER FOOTBALL FEVER](https://welearnaction.onrender.com/super-football-fever.html)
- [TERMS](https://brainquests.github.io/terms.html)
- [TANKS MERGE TANK WAR BLITZ](https://welearnaction.onrender.com/tanks-merge-tank-war-blitz.html)
- [STYLE ICONS 2024 REWIND EDITION](https://learnaction.netlify.app/style-icons-2024-rewind-edition.html)
- [PLANET MERGE](https://welearnaction.onrender.com/planet-merge.html)
- [CONNECT THE DOTS COLOR LINES](https://learnaction.netlify.app/connect-the-dots-color-lines.html)
- [CARJAMCOLOR](https://welearnaction.onrender.com/carjamcolor.html)
- [SMASH THE CAR TO PIECES](https://welearnaction.onrender.com/smash-the-car-to-pieces.html)
- [MAX CRUSHER CRAZY DESTRUCTION AND CAR CRASHES](https://learnaction.netlify.app/max-crusher-crazy-destruction-and-car-crashes.html)
- [ONLINE PORTAL](https://cryptotify.github.io/)
- [ELLIE S RECIPE DUBAI CHOCOLATE BAR](https://welearnaction.onrender.com/ellie-s-recipe-dubai-chocolate-bar.html)
- [SNAKE PUZZLE 3D](https://learnaction.netlify.app/snake-puzzle-3d.html)
- [XYTRIAN RUNNER](https://learnaction.netlify.app/xytrian-runner.html)
- [DESIGN WITH ME SUPERHERO TUTU OUTFITS](https://learnaction.netlify.app/design-with-me-superhero-tutu-outfits.html)
- [DOGGO DROP](https://learnaction.netlify.app/doggo-drop.html)
- [CATEGORY EDUCATIONAL](https://learnaction.netlify.app/category-educational.html)
- [NINE CARDS OF WINTER](https://learnaction.netlify.app/nine-cards-of-winter.html)
- [CRAZY TRAFFIC CONTROL](https://welearnaction.onrender.com/crazy-traffic-control.html)
- [SITEMAP](https://quizverses.pages.dev/sitemap.html)
- [COLOR DOTS CHALLENGE](https://welearnaction.onrender.com/color-dots-challenge.html)
- [GUINEA PIGGY MATCHING](https://welearnaction.onrender.com/guinea-piggy-matching.html)
- [HERO STORY MONSTERS CROSSING](https://welearnaction.onrender.com/hero-story-monsters-crossing.html)
- [CUT N FILL](https://learnaction.netlify.app/cut-n-fill.html)
- [VORTEX IO](https://learnaction.netlify.app/vortex-io.html)
- [DREAM MANIA HAPPY MATCH](https://learnaction.netlify.app/dream-mania-happy-match.html)
- [ELLIE AND BEN CHRISTMAS EVE](https://learnaction.github.io/ellie-and-ben-christmas-eve.html)
- [CATEGORY FOOTBALL](https://learnaction.netlify.app/category-football.html)
- [SUPERMARKET CASHIER SIMULATOR](https://learnaction.github.io/supermarket-cashier-simulator.html)
- [CASTLE CRAFT](https://learnaction.netlify.app/castle-craft.html)
- [BOTTLE CHALLENGE](https://welearnaction.onrender.com/bottle-challenge.html)
- [MACHINE CITY BALLS](https://learnaction.netlify.app/machine-city-balls.html)
- [ONLINE PORTAL](https://cryptotify.pages.dev/)
- [MATH LAVA TOWER RACE](https://welearnaction.onrender.com/math-lava-tower-race.html)
- [TERMS](https://brainquests-fb2c5.web.app/terms.html)
- [SPACE STRIKE GALAXY SHOOTER](https://welearnaction.onrender.com/space-strike-galaxy-shooter.html)
- [IDLE AIRPORT CEO](https://learnaction.github.io/idle-airport-ceo.html)
- [JEWEL COLORING](https://learnaction.netlify.app/jewel-coloring.html)
- [LUNAR PHASE BATTLE](https://welearnaction.onrender.com/lunar-phase-battle.html)
- [MY GARDEN JOURNEY](https://learnaction.github.io/my-garden-journey.html)
- [GT MICRO RACERS](https://welearnaction.onrender.com/gt-micro-racers.html)
- [CATEGORY STICKMAN](https://learnaction.netlify.app/category-stickman.html)
- [STICKMAN ZOMBIE VS STICKMAN HERO](https://learnaction.github.io/stickman-zombie-vs-stickman-hero.html)
- [EVERYTHING IS IN PLACE RARE FINDS](https://welearnaction.onrender.com/everything-is-in-place-rare-finds.html)
- [ROPE STITCH PUZZLE](https://learnaction.github.io/rope-stitch-puzzle.html)
- [STICKMAN KOMBAT 2D](https://learnaction.github.io/stickman-kombat-2d.html)
- [DTA 2 MANIAC](https://learnaction.netlify.app/dta-2-maniac.html)
- [FRUIT MERGE RELOADED](https://learnaction.github.io/fruit-merge-reloaded.html)
- [CRAZYZOMBIES 3D](https://learnaction.github.io/crazyzombies-3d.html)
- [THATS NOT MY NEIGHBOR](https://welearnaction.onrender.com/thats-not-my-neighbor.html)
- [MERGE 3D MATCH 3 BALLOONS](https://learnaction.netlify.app/merge-3d-match-3-balloons.html)
- [EAT AND GROW FISH](https://welearnaction.onrender.com/eat-and-grow-fish.html)
- [FIGHT TO THE END](https://learnaction.netlify.app/fight-to-the-end.html)
- [TEAM LOYALTY](https://learnaction.netlify.app/team-loyalty.html)
- [MERGE BRICK BREAKER](https://learnaction.netlify.app/merge-brick-breaker.html)
- [MERGE WAR](https://learnaction.github.io/merge-war.html)
- [UNDERWATER SURVIVAL](https://learnaction.netlify.app/underwater-survival.html)
- [MERGE BALLS NEW YEARS TOYS IN 3D](https://welearnaction.onrender.com/merge-balls-new-years-toys-in-3d.html)
- [THRILL ROLLER COASTER](https://welearnaction.onrender.com/thrill-roller-coaster.html)
- [SITEMAP](https://cryptotify9.onrender.com/sitemap.html)
- [CATEGORY SKILL256](https://learnaction.netlify.app/category-skill256.html)
- [CUBE CONNECT](https://learnaction.github.io/cube-connect.html)
- [ONLINE PORTAL](https://brainquests.netlify.app/)
- [FIND OBJECTS HIDDEN ITEM](https://welearnaction.onrender.com/find-objects-hidden-item.html)
- [IMAGE CROSSWORD](https://learnaction.netlify.app/image-crossword.html)
- [ROOM SORT FLOOR PLAN](https://learnaction.netlify.app/room-sort-floor-plan.html)
- [BLACK CAT STACKING POP](https://learnaction.github.io/black-cat-stacking-pop.html)
- [CATEGORY TOWER DEFENSE 2](https://welearnaction.onrender.com/category-tower-defense-2.html)
- [HIDDEN OBJECTS VACATION IN BRAZIL](https://welearnaction.onrender.com/hidden-objects-vacation-in-brazil.html)
- [CATEGORY FLASH](https://learnaction.netlify.app/category-flash.html)
- [MONEY FACTORY TYCOON IDLE GAME](https://learnaction.netlify.app/money-factory-tycoon-idle-game.html)
- [THUMBPINBALL](https://learnaction.github.io/thumbpinball.html)
- [TERMS](https://cryptotify.github.io/terms.html)
- [SUPER MX LAST SEASON](https://welearnaction.onrender.com/super-mx-last-season.html)
- [LIVE 100 DAYS](https://welearnaction.onrender.com/live-100-days.html)
- [OBBY 3D SPRUNKI PARKOUR](https://learnaction.netlify.app/obby-3d-sprunki-parkour.html)
- [LAZY DOG](https://welearnaction.onrender.com/lazy-dog.html)
- [MATCH ARENA](https://learnaction.github.io/match-arena.html)
