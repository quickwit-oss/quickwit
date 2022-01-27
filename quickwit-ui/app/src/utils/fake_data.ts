// Copyright (C) 2021 Quickwit, Inc.
//
// Quickwit is offered under the AGPL v3.0 and as commercial software.
// For commercial licensing, contact us at hello@quickwit.io.
//
// AGPL:
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as
// published by the Free Software Foundation, either version 3 of the
// License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program. If not, see <http://www.gnu.org/licenses/>.

import { SearchResponse } from "./models";

export const INDEXES_METADATA = [
  {
    index_id: "wikipedia",
    index_uri: "s3://my-bucket/wikipedia",
    checkpoint: {},
    doc_mapping: {
      field_mappings: [
        {
          name: 'title',
          type: 'string',
        },
        {
          name: 'body',
          type: 'string',
        },
        {
          name: 'url',
          type: 'string',
        }
      ],
      tag_fields: [],
      store: false,
    },
    indexing_settings: {
      timestamp_field: null
    },
    search_settings: {},
    sources: [],
    create_timestamp: Date.now() - 1e6,
    update_timestamp: Date.now(),
    num_docs: 400_000_000,
    num_bytes: 300_000_000,
    num_splits: 30,
  },
  {
    index_id: "hdfs-logs",
    index_uri: "s3://my-bucket/hdfs-logs",
    checkpoint: {},
    doc_mapping: {
      field_mappings: [
        {
          "name": "timestamp",
          "type": "i64",
        },
        {
          "name": "tenant_id",
          "type": "u64",
        },
        {
          "name": "severity_text",
          "type": "text",
        },
        {
          "name": "body",
          "type": "text",
        },
        {
          "name": "resource",
          "type": "object",
          "field_mappings": [
            {
              "name": "service",
              "type": "text",
            }
          ]
        }
      ],
      tag_fields: ['tenant_id'],
      store: false,
    },
    indexing_settings: {
      timestamp_field: 'timestamp'
    },
    search_settings: {},
    sources: [],
    create_timestamp: Date.now() - 1e6,
    update_timestamp: Date.now(),
    num_docs: 400_000_000,
    num_bytes: 300_000_000,
    num_splits: 30,
  },
  {
    index_id: "gh-archive",
    index_uri: "s3://my-bucket/gh-archive",
    checkpoint: {},
    doc_mapping: {
      field_mappings: [
        {
          "name": "id",
          "type": "u64",
        },
        {
          "name": "created_at",
          "type": "i64",
        },
        {
          "name": "event_type",
          "type": "text",
        },
        {
          "name": "title",
          "type": "text",
        },
        {
          "name": "body",
          "type": "text",
        }
      ],
      tag_fields: [],
      store: false, 
    },
    indexing_settings: {
      timestamp_field: 'created_at'
    },
    search_settings: {},
    sources: [],
    create_timestamp: Date.now() - 1e6,
    update_timestamp: Date.now(),
    num_docs: 400_000_000,
    num_bytes: 300_000_000,
    num_splits: 30,
  }
];

export const WIKIPEDIA_SEARCH_RESPONSE: SearchResponse = {
  count: 3,
  hits: [
    {
      url: "https://en.wikipedia.org/wiki?curid=48687903",
      title: "Jeon Hye-jin (actress, born 1988)",
      body: "\nJeon Hye-jin (actress, born 1988)\n\nJeon Hye-jin (born June 17, 1988) is a South Korean actress.\nPersonal life.\nJeon married his \"Smile, You\" co-star Lee Chun-hee on March 11, 2011. Their daughter, Lee So Yu, was born on July 30, 2011.\n\n"
    },
    {
      url: "https://en.wikipedia.org/wiki?curid=48687919",
      title: "Benham, Indiana",
      body: "\nBenham, Indiana\n\nBenham is an unincorporated community in Ripley County, in the U.S. state of Indiana.\nHistory.\nAn old variant name of the community was Benhams Store. A post office opened under the name Benham Store in 1866, the name was shortened to Benham 1888, and the post office was discontinued in 1934. John Benham, Jr., served as a first postmaster. A post office opened under the name Benham Store in 1866, the name was shortened to Benham 1888, and the post office was discontinued in 1934. John Benham, Jr., served as a first postmaster. is an unincorporated community in Ripley County, in the U.S. state of Indiana.\nHistory.\nAn old variant name of the community was Benhams Store. A post office opened under the name Benham Store in 1866, the name was shortened to Benham 1888, and the post office was discontinued in 1934. John Benham, Jr., served as a first postmaster. A post office opened under the name Benham Store in 1866, the name was shortened to Benham 1888, and the post office was discontinued in 1934. John Benham, Jr., served as a first postmaster. is an unincorporated community in Ripley County, in the U.S. state of Indiana.\nHistory.\nAn old variant name of the community was Benhams Store. A post office opened under the name Benham Store in 1866, the name was shortened to Benham 1888, and the post office was discontinued in 1934. John Benham, Jr., served as a first postmaster. A post office opened under the name Benham Store in 1866, the name was shortened to Benham 1888, and the post office was discontinued in 1934. John Benham, Jr., served as a first postmaster.\n\n"
    },
    {
      url: "https://en.wikipedia.org/wiki?curid=48687930",
      title: "Clinton, Ripley County, Indiana",
      body: "\nClinton, Ripley County, Indiana\n\nClinton is an unincorporated community in Ripley County, in the U.S. state of Indiana.\nHistory.\nClinton was founded in 1833.\n\n"
    }
  ],
  numMicrosecs: 1.2
}

export const HDFS_LOGS_SEARCH_RESPONSE: SearchResponse = {
  count: 12,
  hits: [
    {"timestamp":1460530013,"severity_text":"INFO","body":"PacketResponder: BP-108841162-10.10.34.11-1440074360971:blk_1074072698_331874, type=HAS_DOWNSTREAM_IN_PIPELINE terminating","resource":{"service":"datanode/01"},"attributes":{"class":"org.apache.hadoop.hdfs.server.datanode.DataNode"},"tenant_id":25},
    {"timestamp":1460530014,"severity_text":"INFO","body":"Receiving BP-108841162-10.10.34.11-1440074360971:blk_1074072706_331882 src: /10.10.34.33:42666 dest: /10.10.34.11:50010","resource":{"service":"datanode/01"},"attributes":{"class":"org.apache.hadoop.hdfs.server.datanode.DataNode"},"tenant_id":82},
    {"timestamp":1460530014,"severity_text":"INFO","body":"Receiving BP-108841162-10.10.34.11-1440074360971:blk_1074072709_331885 src: /10.10.34.30:33078 dest: /10.10.34.11:50010","resource":{"service":"datanode/01"},"attributes":{"class":"org.apache.hadoop.hdfs.server.datanode.DataNode"},"tenant_id":55},
    {"timestamp":1460530014,"severity_text":"INFO","body":"src: /10.10.34.33:42666, dest: /10.10.34.11:50010, bytes: 272, op: HDFS_WRITE, cliID: DFSClient_NONMAPREDUCE_1888087477_101, offset: 0, srvID: d9ef1b17-4314-4cd8-91eb-095413c3427f, blockid: BP-108841162-10.10.34.11-1440074360971:blk_1074072706_331882, duration: 4236902","resource":{"service":"datanode/01"},"attributes":{"class":"org.apache.hadoop.hdfs.server.datanode.DataNode.clienttrace"},"tenant_id":72},
    {"timestamp":1460530014,"severity_text":"INFO","body":"PacketResponder: BP-108841162-10.10.34.11-1440074360971:blk_1074072706_331882, type=HAS_DOWNSTREAM_IN_PIPELINE terminating","resource":{"service":"datanode/01"},"attributes":{"class":"org.apache.hadoop.hdfs.server.datanode.DataNode"},"tenant_id":61},
    {"timestamp":1460530014,"severity_text":"INFO","body":"src: /10.10.34.30:33078, dest: /10.10.34.11:50010, bytes: 234, op: HDFS_WRITE, cliID: DFSClient_NONMAPREDUCE_-202827006_103, offset: 0, srvID: d9ef1b17-4314-4cd8-91eb-095413c3427f, blockid: BP-108841162-10.10.34.11-1440074360971:blk_1074072709_331885, duration: 2571934","resource":{"service":"datanode/01"},"attributes":{"class":"org.apache.hadoop.hdfs.server.datanode.DataNode.clienttrace"},"tenant_id":68},
    {"timestamp":1460530014,"severity_text":"INFO","body":"PacketResponder: BP-108841162-10.10.34.11-1440074360971:blk_1074072709_331885, type=HAS_DOWNSTREAM_IN_PIPELINE terminating","resource":{"service":"datanode/01"},"attributes":{"class":"org.apache.hadoop.hdfs.server.datanode.DataNode"},"tenant_id":100},
    {"timestamp":1460530014,"severity_text":"INFO","body":"Receiving BP-108841162-10.10.34.11-1440074360971:blk_1074072723_331899 src: /10.10.34.11:34594 dest: /10.10.34.11:50010","resource":{"service":"datanode/01"},"attributes":{"class":"org.apache.hadoop.hdfs.server.datanode.DataNode"},"tenant_id":23},
    {"timestamp":1460530014,"severity_text":"INFO","body":"Receiving BP-108841162-10.10.34.11-1440074360971:blk_1074072731_331907 src: /10.10.34.11:34596 dest: /10.10.34.11:50010","resource":{"service":"datanode/01"},"attributes":{"class":"org.apache.hadoop.hdfs.server.datanode.DataNode"},"tenant_id":48},
    {"timestamp":1440670514,"severity_text":"INFO","body":"PacketResponder: BP-108841162-10.10.34.11-1440074360971:blk_1073837169_96345, type=HAS_DOWNSTREAM_IN_PIPELINE terminating","resource":{"service":"datanode/02"},"attributes":{"class":"org.apache.hadoop.hdfs.server.datanode.DataNode"},"tenant_id":45},
    {"timestamp":1440670515,"severity_text":"INFO","body":"Receiving BP-108841162-10.10.34.11-1440074360971:blk_1073837202_96378 src: /10.10.34.13:54574 dest: /10.10.34.12:50010","resource":{"service":"datanode/02"},"attributes":{"class":"org.apache.hadoop.hdfs.server.datanode.DataNode"},"tenant_id":47},
    {"timestamp":1440670515,"severity_text":"INFO","body":"src: /10.10.34.13:54574, dest: /10.10.34.12:50010, bytes: 61790, op: HDFS_WRITE, cliID: DFSClient_NONMAPREDUCE_-1482587964_1, offset: 0, srvID: 4f8dd80e-ab80-41ad-b045-99cfeb1828d2, blockid: BP-108841162-10.10.34.11-1440074360971:blk_1073837202_96378, duration: 2600882","resource":{"service":"datanode/02"},"attributes":{"class":"org.apache.hadoop.hdfs.server.datanode.DataNode.clienttrace"},"tenant_id":35},
  ],
  numMicrosecs: 0.3
}

export const GH_ARCHIVE_SEARCH_RESPONSE: SearchResponse = {
  count: 11,
  hits: [
    {"id":19541174226,"event_type":"IssueCommentEvent","actor_login":"sffc","repo_name":"rust-diplomat/diplomat","created_at":1640995200,"action":"created","number":120,"title":"Misleading error message on `Option<usize>`","labels":[],"ref":null,"additions":null,"deletions":null,"commit_id":null,"body":"ICU4X diplomat is b448e1e272b70eae1cb2e6b7d0a2f0c68905d49a which I misread as being the latest Diplomat, but it is actually the parent of the latest diplomat.  And the latest commit did a bunch of stuff with `Option`, so it makes sense that this is probably fixed."},
    {"id":19541174243,"event_type":"IssueCommentEvent","actor_login":"github-learning-lab[bot]","repo_name":"desfolio/github-upload","created_at":1640995200,"action":"created","number":1,"title":"Planning the upload to GitHub","labels":[],"ref":null,"additions":null,"deletions":null,"commit_id":null,"body":"Great! I've opened a [new issue](https://github.com/desfolio/github-upload/issues/2) for you.\n\n<hr>\n<h3 align=\"center\">Go to the next issue <a href=\"https://github.com/desfolio/github-upload/issues/2\">here</a>!</h3>\n"},
    {"id":19541174275,"event_type":"IssuesEvent","actor_login":"jilleJr","repo_name":"dinkur/dinkur","created_at":1640995201,"action":"closed","number":21,"title":"Add license notice to CLI","labels":["p/high","t/feature"],"ref":null,"additions":null,"deletions":null,"commit_id":null,"body":"The CLI is missing the license notice. There's no way to obtain the license.\r\n\r\nOne of the clauses in the GPL-3.0 states that if the application does not present its own license, then derivates doesn't need to either.\r\n\r\nNeeds to comply with this. For example by adding flags to the root command:\r\n\r\n```\r\n   --license-c    show license conditions\r\n   --license-w    show license warranty\r\n```\r\n\r\nAlso need the license header to the command.\r\n\r\nGood idea to consider the licenses of dependencies. Include their licenses? Embed their licenses? Or simply refer to THIRD-PARTY-LICENSES.md from the releases page?"},
    {"id":19541174434,"event_type":"IssueCommentEvent","actor_login":"Lee-Carre","repo_name":"kylecorry31/Trail-Sense","created_at":1640995203,"action":"created","number":1099,"title":"Tidal Data Sources","labels":[],"ref":null,"additions":null,"deletions":null,"commit_id":null,"body":"> In the US, constituents can be gathered [from NOAA]\r\n\r\nI tried to find sources of tidal data for Britain, which yielded the following:\r\n* [National Oceanography Centre](http://www.noc.ac.uk/facilities/data-research-facilities)\r\n* [National Tidal and Sea Level Facility](http://www.ntslf.org/)\r\n\r\nAnnoyingly, it seems that not only aren't the harmonic constituents published, but even raw tide height measurements aren't available. Only the resulting predictions (for maybe 2 dozen days). (Mutters something unflattering & contemptuous about British attitudes toward data-hoarding, job-security, & control-freakery. Then something insulting about Ordinance Survey.) Of course, they're only too happy to sell licenses & consulting services, instead.\r\n\r\nSeems that one is expected to both collect your own data, and then perform the harmonic analysis yourself.\r\n\r\nWhat a joke, compared to NOAA. Sigh.\r\n\r\nHowever, in my searching, I came across [CORE](http://www.core.ac.uk) (seemingly a British equivalent to ArXiv, hosting research papers), which hints at revealing more details in some of the documents."},
    {"id":19541174534,"event_type":"ReleaseEvent","actor_login":"getpremia","repo_name":"getpremia/premia-demo","created_at":1640995204,"action":"published","number":null,"title":null,"labels":[],"ref":null,"additions":null,"deletions":null,"commit_id":null,"body":"Automatically set new version to 1.0.8.7.5"},
    {"id":19541174568,"event_type":"IssueCommentEvent","actor_login":"github-actions[bot]","repo_name":"beadth/RSSHub","created_at":1640995204,"action":"created","number":265,"title":"[pull] master from DIYgod:master","labels":[":arrow_heading_down: pull","Auto: Route No Found","merge-conflict"],"ref":null,"additions":null,"deletions":null,"commit_id":null,"body":"自动检测失败, 请确认PR正文部分符合格式规范并重新开启, 详情请检查日志\nAuto Route test failed, please check your PR body format and reopen pull request. Check logs for more details"},
    {"id":19541174692,"event_type":"PullRequestEvent","actor_login":"github-actions[bot]","repo_name":"beadth/RSSHub","created_at":1640995205,"action":"closed","number":265,"title":"[pull] master from DIYgod:master","labels":[":arrow_heading_down: pull","Auto: Route No Found","merge-conflict"],"ref":null,"additions":17684,"deletions":48494,"commit_id":null,"body":"See [Commits](/beadth/RSSHub/pull/265/commits) and [Changes](/beadth/RSSHub/pull/265/files) for more details.\n\n-----\nCreated by [<img src=\"https://prod.download/pull-18h-svg\" valign=\"bottom\"/> **pull[bot]**](https://github.com/wei/pull)\n\n_Can you help keep this open source service alive? **[💖 Please sponsor : )](https://prod.download/pull-pr-sponsor)**_"},
    {"id":19541174733,"event_type":"ReleaseEvent","actor_login":"github-actions[bot]","repo_name":"oltranehar3/proteport","created_at":1640995205,"action":"published","number":null,"title":null,"labels":[],"ref":null,"additions":null,"deletions":null,"commit_id":null,"body":""},
    {"id":19541174863,"event_type":"IssueCommentEvent","actor_login":"mrbbot","repo_name":"cloudflare/miniflare","created_at":1640995206,"action":"created","number":130,"title":"Emulate racing in KV","labels":[],"ref":null,"additions":null,"deletions":null,"commit_id":null,"body":"Hey! 👋 I like this idea, though I'm not sure about adding non-determinism to tests by default, even though this probably means bugs in code. For development though via the CLI, it could be a good default. I feel like if it wasn't the default, people wouldn't enable it.\n\nAs for the implementation, we could probably do it as a decorator on the KVNamespace class. Maybe something to look at once V2 is fully-released?\n\nHappy new year btw 🎉"},
    {"id":19541174876,"event_type":"PullRequestEvent","actor_login":"patrickjohanndwyer","repo_name":"patrickjohanndwyer/story2","created_at":1640995206,"action":"closed","number":1,"title":"Update chapter1.txt","labels":[],"ref":null,"additions":1,"deletions":0,"commit_id":null,"body":"update chapter 1 from experimental branch for a better story."},
    {"id":19541174938,"event_type":"PullRequestEvent","actor_login":"atomista[bot]","repo_name":"smokey-org/atomist-docker-tutorial","created_at":1640995207,"action":"opened","number":3342,"title":"Pin Docker base image in Dockerfile","labels":[],"ref":null,"additions":1,"deletions":1,"commit_id":null,"body":"This pull request pins the Docker base image `node:14-buster-slim` in [`Dockerfile`](https://github.com/smokey-org/atomist-docker-tutorial/blob/2b81342d67bb92d9759f19c6bf2912c476230ab0/Dockerfile) to the current digest.\n\nhttps://github.com/smokey-org/atomist-docker-tutorial/blob/2b81342d67bb92d9759f19c6bf2912c476230ab0/Dockerfile#L1-L1\n\n<!-- atomist:hide -->\nDigest `sha256:20bedf0c09de887379e59a41c04284974f5fb529cf0e13aab613473ce298da3d` references a [multi-CPU architecture image manifest](https://docs.docker.com/desktop/multi-arch/). This image supports the following architectures:\n\n* [<code>sha256:fb03437fb9e4451d583defce033bb65b3b02fdcfb09a94f92e7d76eae35ecb9c</code>](https://hub.docker.com/layers/node/library/node/14-buster-slim/images/sha256-fb03437fb9e4451d583defce033bb65b3b02fdcfb09a94f92e7d76eae35ecb9c) <code>linux/amd64</code>\n<!-- atomist:show -->\n\n---\n\n<!-- atomist:hide -->\nPinning `FROM` lines to digests makes your builds repeatable. Atomist will raise new pull requests whenever the tag moves, so that you know when the base image has been updated. You can follow a new tag at any time. Just replace the digest with the new tag you want to follow. Atomist, will switch to following this new tag.\n<!-- atomist:show -->\n\n---\n\nFile changed:\n\n-   [`Dockerfile`](https://github.com/smokey-org/atomist-docker-tutorial/blob/atomist/pin-docker-base-image/dockerfile/Dockerfile)\n\n<!-- atomist:hide -->\n\n<!-- atomist:show -->\n\n\n<!--\n  [atomist:generated]\n  [atomist-skill:atomist/docker-base-image-policy]\n  [atomist-version:0.1.88]\n  [atomist-configuration:policy-cfg]\n  [atomist-workspace-id:A31EIG51O]\n  [atomist-tx:601]\n  [atomist-correlation-id:839d6cd1-b218-4a37-93c0-2a254380becd.eY0EYTtuhanoWv8u0tRWH]\n  [atomist-diff:9f8b357a5fe128d65caf09acab97d08270c6f7a6b199353074534abfb46f8685]\n-->"},  
  ],
  numMicrosecs: 0.6
}