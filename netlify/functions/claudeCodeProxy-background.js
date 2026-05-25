/* netlify/functions/claudeCodeProxy-background.js */
/* ═══════════════════════════════════════════════════════════════════
   TRANCHED AI PIPELINE — v5.2 (+ Spec Validation Patch Loop)
   ─────────────────────────────────────────────────────────────────
   Each invocation handles ONE unit of work then chains to itself
   for the next, staying well under Netlify's 15-min limit.

   Invocation 0    ▸  "plan"    — Spec Validation Gate (3 Sonnet calls)
                       runs first, then Opus 4.6 creates a dependency-
                       ordered, contract-driven tranche plan.
                       Gate FAIL writes ai_error.json with structured issues
                       and halts before Opus fires.
   Invocation 1–N  ▸  "tranche" — Sonnet 4.6 executes one tranche,
                       saves accumulated files, chains to next tranche.
   Correction loop ▸  "fix"     — Used only for objective retryable
                       validation failures.
   Final           ▸  Writes ai_response.json for frontend pickup.

   SPEC VALIDATION GATE (runs in "plan" mode before Opus):
   ─────────────────────────────────────────────────────────
   Call 1 — Extract  : Sonnet reads Master Prompt, produces 6-8 custom
                       simulation scenarios specific to this game's mechanics and current prompt layout.
   Call 2 — Simulate : Sonnet traces each scenario through the spec rules
                       literally, documents findings in plain text.
   Call 3 — Review   : Sonnet classifies findings as PASS/FAIL JSON.
   On PASS  → continues to Opus planning as normal.
   On FAIL  → writes ai_error.json { validationFailed:true, issues:[...] }
              and returns without invoking Opus. Frontend renders issues
              in the tranche panel so the user can fix the Master Prompt.
   On error → validation is skipped with a warning; Opus proceeds.

   Recovery policy:
   - 0 retries for soft/advisory findings.
   - 1 retry max for narrow objective hard failures when the repair is surgical.
   - 2 retries only for parser/envelope failures or truly critical scaffold/runtime issues.

   All intermediate state lives in Firebase so each invocation is
   stateless and can reconstruct context from the pipeline file.
   ═══════════════════════════════════════════════════════════════════ */

const fetch = require("node-fetch");
const JSZip = require("jszip");
const admin = require("./firebaseAdmin");

const RETRY_POLICY = Object.freeze({
  parser_envelope: 2,
  critical_runtime: 2  // retained for fix-mode retryBudget fallback
});

const SCAFFOLD_CHOOSER_MODEL = process.env.SCAFFOLD_CHOOSER_MODEL || 'claude-opus-4-7';

/* ── Scaffold chooser payload caps ────────────────────────────────
   Hard limits that protect the Opus 4.7 chooser from two failure
   modes: (a) unbounded candidate count silently dropping overlays,
   (b) an individual overlay file being large enough to blow the
   request past the 6 MB Netlify limit or eat the Opus output budget
   on prompt echoing.

   Candidate count is a HARD FAIL on overflow. Do NOT silently slice
   — that is a deterministic fallback (it deterministically picks
   "the first N"). If a library grows past the cap, the human must
   explicitly curate which overlays are eligible.

   Per-overlay text IS truncated with a visible marker so Opus can
   still see and score every candidate. Truncation is lossy but the
   remaining head of the overlay (signature, anchors, prohibitions,
   session loop) is the decision-driving content and lives at the
   top of a well-formed overlay. The truncation marker is explicit
   so Opus knows it is not reading the full document.
*/
const SCAFFOLD_CHOOSER_MAX_CANDIDATES = 24;
const SCAFFOLD_CHOOSER_MAX_OVERLAY_CHARS = 12000;
const SCAFFOLD_CHOOSER_TRUNCATION_MARKER =
  '\n\n[... OVERLAY TEXT TRUNCATED BY CHOOSER PAYLOAD CAP — SCORE FROM HEAD SECTION ABOVE ...]';

const ROAD_PIPELINE_ZIP_PATH = "game-generator-1/projects/BASE_Files/asset_3d_objects/Road.zip";
const ROAD_PIPELINE_INDEX_ENTRY_NAMES = Object.freeze(["Road_Index.json", "road_index.json"]);
const COMPILER_OWNED_JSON_PATHS = Object.freeze(["json/assets.json", "json/tree.json", "json/entities.json"]);

const FALLBACK_ROAD_INDEX = Object.freeze({
  "roadBump": {
    "filename": "roadBump.obj",
    "type": "bump",
    "tags": [
      "bump",
      "long"
    ],
    "dimensions": {
      "length": 20,
      "width": 10,
      "height": 1.0
    },
    "entry": {
      "face": "z-",
      "x_left": -10,
      "x_right": 0,
      "y": 0
    },
    "exit": {
      "face": "z",
      "x_left": -10,
      "x_right": 0,
      "x_drift": 0,
      "y_rise": 0
    },
    "slots": [
      {
        "slot": 0,
        "material": "grass",
        "r": 90,
        "g": 154,
        "b": 50
      },
      {
        "slot": 1,
        "material": "grey",
        "r": 160,
        "g": 160,
        "b": 160
      },
      {
        "slot": 2,
        "material": "road",
        "r": 68,
        "g": 68,
        "b": 68
      }
    ],
    "slot_count": 3,
    "colormap": null,
    "material_file": ""
  },
  "roadCornerLarge": {
    "filename": "roadCornerLarge.obj",
    "type": "corner",
    "tags": [
      "corner",
      "x_shift",
      "corner_exit",
      "long"
    ],
    "dimensions": {
      "length": 20,
      "width": 20,
      "height": 0.2
    },
    "entry": {
      "face": "z-",
      "x_left": -10,
      "x_right": 0,
      "y": 0
    },
    "exit": {
      "face": "x-",
      "x_left": -20,
      "x_right": -20,
      "x_drift": -10,
      "y_rise": 0
    },
    "slots": [
      {
        "slot": 0,
        "material": "grey",
        "r": 160,
        "g": 160,
        "b": 160
      },
      {
        "slot": 1,
        "material": "grass",
        "r": 90,
        "g": 154,
        "b": 50
      },
      {
        "slot": 2,
        "material": "road",
        "r": 68,
        "g": 68,
        "b": 68
      }
    ],
    "slot_count": 3,
    "colormap": null,
    "material_file": ""
  },
  "roadCornerLarger": {
    "filename": "roadCornerLarger.obj",
    "type": "corner",
    "tags": [
      "corner",
      "x_shift",
      "corner_exit",
      "long"
    ],
    "dimensions": {
      "length": 30,
      "width": 30,
      "height": 0.2
    },
    "entry": {
      "face": "z-",
      "x_left": -10,
      "x_right": 0,
      "y": 0
    },
    "exit": {
      "face": "x-",
      "x_left": -30,
      "x_right": -30,
      "x_drift": -20,
      "y_rise": 0
    },
    "slots": [
      {
        "slot": 0,
        "material": "grey",
        "r": 160,
        "g": 160,
        "b": 160
      },
      {
        "slot": 1,
        "material": "grass",
        "r": 90,
        "g": 154,
        "b": 50
      },
      {
        "slot": 2,
        "material": "road",
        "r": 68,
        "g": 68,
        "b": 68
      }
    ],
    "slot_count": 3,
    "colormap": null,
    "material_file": ""
  },
  "roadCornerSmall": {
    "filename": "roadCornerSmall.obj",
    "type": "corner",
    "tags": [
      "corner"
    ],
    "dimensions": {
      "length": 10,
      "width": 10,
      "height": 0.2
    },
    "entry": {
      "face": "z-",
      "x_left": -10,
      "x_right": 0,
      "y": 0
    },
    "exit": {
      "face": "z",
      "x_left": -10,
      "x_right": -8.69,
      "x_drift": 0,
      "y_rise": 0
    },
    "slots": [
      {
        "slot": 0,
        "material": "grey",
        "r": 160,
        "g": 160,
        "b": 160
      },
      {
        "slot": 1,
        "material": "grass",
        "r": 90,
        "g": 154,
        "b": 50
      },
      {
        "slot": 2,
        "material": "road",
        "r": 68,
        "g": 68,
        "b": 68
      }
    ],
    "slot_count": 3,
    "colormap": null,
    "material_file": ""
  },
  "roadCornerSmallSquare": {
    "filename": "roadCornerSmallSquare.obj",
    "type": "corner",
    "tags": [
      "corner"
    ],
    "dimensions": {
      "length": 10,
      "width": 10,
      "height": 0.2
    },
    "entry": {
      "face": "z-",
      "x_left": -10,
      "x_right": 0,
      "y": 0
    },
    "exit": {
      "face": "z",
      "x_left": -10,
      "x_right": -1.15,
      "x_drift": 0,
      "y_rise": 0
    },
    "slots": [
      {
        "slot": 0,
        "material": "grass",
        "r": 90,
        "g": 154,
        "b": 50
      },
      {
        "slot": 1,
        "material": "_defaultMat",
        "r": 130,
        "g": 130,
        "b": 130
      },
      {
        "slot": 2,
        "material": "road",
        "r": 68,
        "g": 68,
        "b": 68
      }
    ],
    "slot_count": 3,
    "colormap": null,
    "material_file": ""
  },
  "roadCurved": {
    "filename": "roadCurved.obj",
    "type": "curved",
    "tags": [
      "curved",
      "x_shift",
      "long"
    ],
    "dimensions": {
      "length": 20,
      "width": 15,
      "height": 0.2
    },
    "entry": {
      "face": "z-",
      "x_left": -10,
      "x_right": 0,
      "y": 0
    },
    "exit": {
      "face": "z",
      "x_left": -5,
      "x_right": 5,
      "x_drift": 5,
      "y_rise": 0
    },
    "slots": [
      {
        "slot": 0,
        "material": "road",
        "r": 68,
        "g": 68,
        "b": 68
      },
      {
        "slot": 1,
        "material": "grey",
        "r": 160,
        "g": 160,
        "b": 160
      },
      {
        "slot": 2,
        "material": "grass",
        "r": 90,
        "g": 154,
        "b": 50
      }
    ],
    "slot_count": 3,
    "colormap": null,
    "material_file": ""
  },
  "roadEnd": {
    "filename": "roadEnd.obj",
    "type": "end",
    "tags": [
      "end",
      "x_shift",
      "corner_exit"
    ],
    "dimensions": {
      "length": 15,
      "width": 10,
      "height": 0.2
    },
    "entry": {
      "face": "z-",
      "x_left": -10,
      "x_right": 0,
      "y": 0
    },
    "exit": {
      "face": "x-",
      "x_left": -5,
      "x_right": -5,
      "x_drift": 5,
      "y_rise": 0
    },
    "slots": [
      {
        "slot": 0,
        "material": "grey",
        "r": 160,
        "g": 160,
        "b": 160
      },
      {
        "slot": 1,
        "material": "grass",
        "r": 90,
        "g": 154,
        "b": 50
      },
      {
        "slot": 2,
        "material": "road",
        "r": 68,
        "g": 68,
        "b": 68
      }
    ],
    "slot_count": 3,
    "colormap": null,
    "material_file": ""
  },
  "roadHalfCircle": {
    "filename": "roadHalfCircle.obj",
    "type": "hairpin",
    "tags": [
      "hairpin",
      "x_shift"
    ],
    "dimensions": {
      "length": 15,
      "width": 30,
      "height": 0.2
    },
    "entry": {
      "face": "z-",
      "x_left": -30,
      "x_right": 0,
      "y": 0
    },
    "exit": {
      "face": "z",
      "x_left": -15,
      "x_right": -15,
      "x_drift": 15,
      "y_rise": 0
    },
    "slots": [
      {
        "slot": 0,
        "material": "grass",
        "r": 90,
        "g": 154,
        "b": 50
      },
      {
        "slot": 1,
        "material": "grey",
        "r": 160,
        "g": 160,
        "b": 160
      },
      {
        "slot": 2,
        "material": "road",
        "r": 68,
        "g": 68,
        "b": 68
      }
    ],
    "slot_count": 3,
    "colormap": null,
    "material_file": ""
  },
  "roadPitEntry": {
    "filename": "roadPitEntry.obj",
    "type": "pit",
    "tags": [
      "pit",
      "x_shift",
      "long"
    ],
    "dimensions": {
      "length": 20,
      "width": 20,
      "height": 0.2
    },
    "entry": {
      "face": "z-",
      "x_left": -10,
      "x_right": 0,
      "y": 0
    },
    "exit": {
      "face": "z",
      "x_left": -20,
      "x_right": 0,
      "x_drift": -10,
      "y_rise": 0
    },
    "slots": [
      {
        "slot": 0,
        "material": "grey",
        "r": 160,
        "g": 160,
        "b": 160
      },
      {
        "slot": 1,
        "material": "grass",
        "r": 90,
        "g": 154,
        "b": 50
      },
      {
        "slot": 2,
        "material": "road",
        "r": 68,
        "g": 68,
        "b": 68
      }
    ],
    "slot_count": 3,
    "colormap": null,
    "material_file": ""
  },
  "roadPitGarage": {
    "filename": "roadPitGarage.obj",
    "type": "pit",
    "tags": [
      "pit"
    ],
    "dimensions": {
      "length": 10,
      "width": 10,
      "height": 0.2
    },
    "entry": {
      "face": "z-",
      "x_left": -10,
      "x_right": 0,
      "y": 0
    },
    "exit": {
      "face": "z",
      "x_left": -10,
      "x_right": 0,
      "x_drift": 0,
      "y_rise": 0
    },
    "slots": [
      {
        "slot": 0,
        "material": "grey",
        "r": 160,
        "g": 160,
        "b": 160
      },
      {
        "slot": 1,
        "material": "road",
        "r": 68,
        "g": 68,
        "b": 68
      },
      {
        "slot": 2,
        "material": "grass",
        "r": 90,
        "g": 154,
        "b": 50
      }
    ],
    "slot_count": 3,
    "colormap": null,
    "material_file": ""
  },
  "roadPitStraight": {
    "filename": "roadPitStraight.obj",
    "type": "pit",
    "tags": [
      "pit"
    ],
    "dimensions": {
      "length": 10,
      "width": 10,
      "height": 0.2
    },
    "entry": {
      "face": "z-",
      "x_left": -10,
      "x_right": 0,
      "y": 0
    },
    "exit": {
      "face": "z",
      "x_left": -10,
      "x_right": 0,
      "x_drift": 0,
      "y_rise": 0
    },
    "slots": [
      {
        "slot": 0,
        "material": "grass",
        "r": 90,
        "g": 154,
        "b": 50
      },
      {
        "slot": 1,
        "material": "grey",
        "r": 160,
        "g": 160,
        "b": 160
      },
      {
        "slot": 2,
        "material": "road",
        "r": 68,
        "g": 68,
        "b": 68
      }
    ],
    "slot_count": 3,
    "colormap": null,
    "material_file": ""
  },
  "roadPitStraightLong": {
    "filename": "roadPitStraightLong.obj",
    "type": "pit",
    "tags": [
      "pit",
      "long"
    ],
    "dimensions": {
      "length": 20,
      "width": 10,
      "height": 0.2
    },
    "entry": {
      "face": "z-",
      "x_left": -10,
      "x_right": 0,
      "y": 0
    },
    "exit": {
      "face": "z",
      "x_left": -10,
      "x_right": 0,
      "x_drift": 0,
      "y_rise": 0
    },
    "slots": [
      {
        "slot": 0,
        "material": "grass",
        "r": 90,
        "g": 154,
        "b": 50
      },
      {
        "slot": 1,
        "material": "grey",
        "r": 160,
        "g": 160,
        "b": 160
      },
      {
        "slot": 2,
        "material": "road",
        "r": 68,
        "g": 68,
        "b": 68
      }
    ],
    "slot_count": 3,
    "colormap": null,
    "material_file": ""
  },
  "roadRamp": {
    "filename": "roadRamp.obj",
    "type": "ramp",
    "tags": [
      "ramp",
      "elevation_change"
    ],
    "dimensions": {
      "length": 10,
      "width": 10,
      "height": 2.7
    },
    "entry": {
      "face": "z-",
      "x_left": -10,
      "x_right": 0,
      "y": 0
    },
    "exit": {
      "face": "z",
      "x_left": -10,
      "x_right": 0,
      "x_drift": 0,
      "y_rise": 2.7
    },
    "slots": [
      {
        "slot": 0,
        "material": "grass",
        "r": 90,
        "g": 154,
        "b": 50
      },
      {
        "slot": 1,
        "material": "grey",
        "r": 160,
        "g": 160,
        "b": 160
      },
      {
        "slot": 2,
        "material": "road",
        "r": 68,
        "g": 68,
        "b": 68
      }
    ],
    "slot_count": 3,
    "colormap": null,
    "material_file": ""
  },
  "roadRampLong": {
    "filename": "roadRampLong.obj",
    "type": "ramp",
    "tags": [
      "ramp",
      "elevation_change",
      "long"
    ],
    "dimensions": {
      "length": 20,
      "width": 10,
      "height": 5.2
    },
    "entry": {
      "face": "z-",
      "x_left": -10,
      "x_right": 0,
      "y": 0
    },
    "exit": {
      "face": "z",
      "x_left": -10,
      "x_right": 0,
      "x_drift": 0,
      "y_rise": 5.2
    },
    "slots": [
      {
        "slot": 0,
        "material": "grass",
        "r": 90,
        "g": 154,
        "b": 50
      },
      {
        "slot": 1,
        "material": "grey",
        "r": 160,
        "g": 160,
        "b": 160
      },
      {
        "slot": 2,
        "material": "road",
        "r": 68,
        "g": 68,
        "b": 68
      }
    ],
    "slot_count": 3,
    "colormap": null,
    "material_file": ""
  },
  "roadRampLongCurved": {
    "filename": "roadRampLongCurved.obj",
    "type": "ramp",
    "tags": [
      "ramp",
      "elevation_change",
      "long"
    ],
    "dimensions": {
      "length": 20,
      "width": 10,
      "height": 5.2
    },
    "entry": {
      "face": "z-",
      "x_left": -10,
      "x_right": 0,
      "y": 0
    },
    "exit": {
      "face": "z",
      "x_left": -10,
      "x_right": 0,
      "x_drift": 0,
      "y_rise": 5.2
    },
    "slots": [
      {
        "slot": 0,
        "material": "grey",
        "r": 160,
        "g": 160,
        "b": 160
      },
      {
        "slot": 1,
        "material": "grass",
        "r": 90,
        "g": 154,
        "b": 50
      },
      {
        "slot": 2,
        "material": "road",
        "r": 68,
        "g": 68,
        "b": 68
      }
    ],
    "slot_count": 3,
    "colormap": null,
    "material_file": ""
  },
  "roadRampLongCurvedWall": {
    "filename": "roadRampLongCurvedWall.obj",
    "type": "ramp",
    "tags": [
      "ramp",
      "elevation_change",
      "long"
    ],
    "dimensions": {
      "length": 20,
      "width": 10,
      "height": 5.2
    },
    "entry": {
      "face": "z-",
      "x_left": -10,
      "x_right": 0,
      "y": 0
    },
    "exit": {
      "face": "z",
      "x_left": -10,
      "x_right": 0,
      "x_drift": 0,
      "y_rise": 5.2
    },
    "slots": [
      {
        "slot": 0,
        "material": "wall",
        "r": 200,
        "g": 180,
        "b": 140
      },
      {
        "slot": 1,
        "material": "grey",
        "r": 160,
        "g": 160,
        "b": 160
      },
      {
        "slot": 2,
        "material": "road",
        "r": 68,
        "g": 68,
        "b": 68
      }
    ],
    "slot_count": 3,
    "colormap": null,
    "material_file": ""
  },
  "roadRampLongWall": {
    "filename": "roadRampLongWall.obj",
    "type": "ramp",
    "tags": [
      "ramp",
      "elevation_change",
      "long"
    ],
    "dimensions": {
      "length": 20,
      "width": 10,
      "height": 5.2
    },
    "entry": {
      "face": "z-",
      "x_left": -10,
      "x_right": 0,
      "y": 0
    },
    "exit": {
      "face": "z",
      "x_left": -10,
      "x_right": 0,
      "x_drift": 0,
      "y_rise": 5.2
    },
    "slots": [
      {
        "slot": 0,
        "material": "wall",
        "r": 200,
        "g": 180,
        "b": 140
      },
      {
        "slot": 1,
        "material": "grey",
        "r": 160,
        "g": 160,
        "b": 160
      },
      {
        "slot": 2,
        "material": "road",
        "r": 68,
        "g": 68,
        "b": 68
      }
    ],
    "slot_count": 3,
    "colormap": null,
    "material_file": ""
  },
  "roadRampWall": {
    "filename": "roadRampWall.obj",
    "type": "ramp",
    "tags": [
      "ramp",
      "elevation_change"
    ],
    "dimensions": {
      "length": 10,
      "width": 10,
      "height": 2.7
    },
    "entry": {
      "face": "z-",
      "x_left": -10,
      "x_right": 0,
      "y": 0
    },
    "exit": {
      "face": "z",
      "x_left": -10,
      "x_right": 0,
      "x_drift": 0,
      "y_rise": 2.7
    },
    "slots": [
      {
        "slot": 0,
        "material": "wall",
        "r": 200,
        "g": 180,
        "b": 140
      },
      {
        "slot": 1,
        "material": "grey",
        "r": 160,
        "g": 160,
        "b": 160
      },
      {
        "slot": 2,
        "material": "road",
        "r": 68,
        "g": 68,
        "b": 68
      }
    ],
    "slot_count": 3,
    "colormap": null,
    "material_file": ""
  },
  "roadSplit": {
    "filename": "roadSplit.obj",
    "type": "split",
    "tags": [
      "split",
      "x_shift",
      "long"
    ],
    "dimensions": {
      "length": 20,
      "width": 30,
      "height": 0.2
    },
    "entry": {
      "face": "z-",
      "x_left": -10,
      "x_right": 0,
      "y": 0
    },
    "exit": {
      "face": "z",
      "x_left": -20,
      "x_right": 10,
      "x_drift": -10,
      "y_rise": 0
    },
    "slots": [
      {
        "slot": 0,
        "material": "grey",
        "r": 160,
        "g": 160,
        "b": 160
      },
      {
        "slot": 1,
        "material": "grass",
        "r": 90,
        "g": 154,
        "b": 50
      },
      {
        "slot": 2,
        "material": "road",
        "r": 68,
        "g": 68,
        "b": 68
      }
    ],
    "slot_count": 3,
    "colormap": null,
    "material_file": ""
  },
  "roadStart": {
    "filename": "roadStart.obj",
    "type": "start",
    "tags": [
      "start",
      "elevation_change",
      "long"
    ],
    "dimensions": {
      "length": 20,
      "width": 12.6,
      "height": 6.68
    },
    "entry": {
      "face": "z-",
      "x_left": -11.3,
      "x_right": -1.3,
      "y": 0
    },
    "exit": {
      "face": "z",
      "x_left": -11.3,
      "x_right": -1.3,
      "x_drift": 0,
      "y_rise": 0
    },
    "slots": [
      {
        "slot": 0,
        "material": "grey",
        "r": 160,
        "g": 160,
        "b": 160
      },
      {
        "slot": 1,
        "material": "grass",
        "r": 90,
        "g": 154,
        "b": 50
      },
      {
        "slot": 2,
        "material": "road",
        "r": 68,
        "g": 68,
        "b": 68
      }
    ],
    "slot_count": 3,
    "colormap": null,
    "material_file": ""
  },
  "roadStartPositions": {
    "filename": "roadStartPositions.obj",
    "type": "start",
    "tags": [
      "start",
      "long"
    ],
    "dimensions": {
      "length": 20,
      "width": 10,
      "height": 0.2
    },
    "entry": {
      "face": "z-",
      "x_left": -10,
      "x_right": 0,
      "y": 0
    },
    "exit": {
      "face": "z",
      "x_left": -10,
      "x_right": 0,
      "x_drift": 0,
      "y_rise": 0
    },
    "slots": [
      {
        "slot": 0,
        "material": "grass",
        "r": 90,
        "g": 154,
        "b": 50
      },
      {
        "slot": 1,
        "material": "grey",
        "r": 160,
        "g": 160,
        "b": 160
      },
      {
        "slot": 2,
        "material": "road",
        "r": 68,
        "g": 68,
        "b": 68
      }
    ],
    "slot_count": 3,
    "colormap": null,
    "material_file": "",
    "sequence_role": "always_first"
  },
  "roadStraight": {
    "filename": "roadStraight.obj",
    "type": "straight",
    "tags": [
      "straight"
    ],
    "dimensions": {
      "length": 10,
      "width": 10,
      "height": 0.2
    },
    "entry": {
      "face": "z-",
      "x_left": -10,
      "x_right": 0,
      "y": 0
    },
    "exit": {
      "face": "z",
      "x_left": -10,
      "x_right": 0,
      "x_drift": 0,
      "y_rise": 0
    },
    "slots": [
      {
        "slot": 0,
        "material": "grass",
        "r": 90,
        "g": 154,
        "b": 50
      },
      {
        "slot": 1,
        "material": "grey",
        "r": 160,
        "g": 160,
        "b": 160
      },
      {
        "slot": 2,
        "material": "road",
        "r": 68,
        "g": 68,
        "b": 68
      }
    ],
    "slot_count": 3,
    "colormap": null,
    "material_file": ""
  },
  "roadStraightBridge": {
    "filename": "roadStraightBridge.obj",
    "type": "bridge",
    "tags": [
      "bridge",
      "elevation_change"
    ],
    "dimensions": {
      "length": 10,
      "width": 10,
      "height": 5.2
    },
    "entry": {
      "face": "z-",
      "x_left": -10,
      "x_right": 0,
      "y": 0
    },
    "exit": {
      "face": "z",
      "x_left": -10,
      "x_right": 0,
      "x_drift": 0,
      "y_rise": -5.2
    },
    "slots": [
      {
        "slot": 0,
        "material": "wall",
        "r": 200,
        "g": 180,
        "b": 140
      },
      {
        "slot": 1,
        "material": "grey",
        "r": 160,
        "g": 160,
        "b": 160
      },
      {
        "slot": 2,
        "material": "road",
        "r": 68,
        "g": 68,
        "b": 68
      },
      {
        "slot": 3,
        "material": "_defaultMat",
        "r": 130,
        "g": 130,
        "b": 130
      }
    ],
    "slot_count": 4,
    "colormap": null,
    "material_file": "",
    "sequence_role": "bridge_end"
  },
  "roadStraightBridgeMid": {
    "filename": "roadStraightBridgeMid.obj",
    "type": "bridge",
    "tags": [
      "bridge",
      "elevation_change"
    ],
    "dimensions": {
      "length": 10,
      "width": 10,
      "height": 1.27
    },
    "entry": {
      "face": "z-",
      "x_left": -10,
      "x_right": 0,
      "y": 0
    },
    "exit": {
      "face": "z",
      "x_left": -10,
      "x_right": 0,
      "x_drift": 0,
      "y_rise": 0
    },
    "slots": [
      {
        "slot": 0,
        "material": "wall",
        "r": 200,
        "g": 180,
        "b": 140
      },
      {
        "slot": 1,
        "material": "grey",
        "r": 160,
        "g": 160,
        "b": 160
      },
      {
        "slot": 2,
        "material": "road",
        "r": 68,
        "g": 68,
        "b": 68
      }
    ],
    "slot_count": 3,
    "colormap": null,
    "material_file": "",
    "sequence_role": "bridge_mid"
  },
  "roadStraightBridgeStart": {
    "filename": "roadStraightBridgeStart.obj",
    "type": "bridge",
    "tags": [
      "bridge",
      "elevation_change"
    ],
    "dimensions": {
      "length": 10,
      "width": 10,
      "height": 5.2
    },
    "entry": {
      "face": "z-",
      "x_left": -10,
      "x_right": 0,
      "y": 0
    },
    "exit": {
      "face": "z",
      "x_left": -10,
      "x_right": 0,
      "x_drift": 0,
      "y_rise": 5.2
    },
    "slots": [
      {
        "slot": 0,
        "material": "wall",
        "r": 200,
        "g": 180,
        "b": 140
      },
      {
        "slot": 1,
        "material": "grey",
        "r": 160,
        "g": 160,
        "b": 160
      },
      {
        "slot": 2,
        "material": "road",
        "r": 68,
        "g": 68,
        "b": 68
      }
    ],
    "slot_count": 3,
    "colormap": null,
    "material_file": "",
    "sequence_role": "bridge_start"
  },
  "roadStraightLong": {
    "filename": "roadStraightLong.obj",
    "type": "straight",
    "tags": [
      "straight",
      "long"
    ],
    "dimensions": {
      "length": 20,
      "width": 10,
      "height": 0.2
    },
    "entry": {
      "face": "z-",
      "x_left": -10,
      "x_right": 0,
      "y": 0
    },
    "exit": {
      "face": "z",
      "x_left": -10,
      "x_right": 0,
      "x_drift": 0,
      "y_rise": 0
    },
    "slots": [
      {
        "slot": 0,
        "material": "grass",
        "r": 90,
        "g": 154,
        "b": 50
      },
      {
        "slot": 1,
        "material": "grey",
        "r": 160,
        "g": 160,
        "b": 160
      },
      {
        "slot": 2,
        "material": "road",
        "r": 68,
        "g": 68,
        "b": 68
      }
    ],
    "slot_count": 3,
    "colormap": null,
    "material_file": ""
  },
  "roadStraightSkew": {
    "filename": "roadStraightSkew.obj",
    "type": "skew",
    "tags": [
      "skew",
      "x_shift",
      "long"
    ],
    "dimensions": {
      "length": 20,
      "width": 20,
      "height": 0.2
    },
    "entry": {
      "face": "z-",
      "x_left": -10,
      "x_right": 0,
      "y": 0
    },
    "exit": {
      "face": "z",
      "x_left": -20,
      "x_right": -10,
      "x_drift": -10,
      "y_rise": 0
    },
    "slots": [
      {
        "slot": 0,
        "material": "grass",
        "r": 90,
        "g": 154,
        "b": 50
      },
      {
        "slot": 1,
        "material": "grey",
        "r": 160,
        "g": 160,
        "b": 160
      },
      {
        "slot": 2,
        "material": "road",
        "r": 68,
        "g": 68,
        "b": 68
      }
    ],
    "slot_count": 3,
    "colormap": null,
    "material_file": ""
  }
});

function detectRoadPipelineSettings(promptText = "", existing = null) {
  const lower = String(promptText || "").toLowerCase();
  const hasAny = (...patterns) => patterns.some(pattern => pattern.test(lower));
  const existingValue = (existing && typeof existing === "object") ? existing : {};

  let gameType = "other";
  if (hasAny(
    /(racing|race car|drift|time trial|lap timer|checkpoint racing)/,
    /(track|circuit|racetrack|raceway|road course)/,
    /(laps|finish line|starting grid|pit lane)/
  )) {
    gameType = "racing";
  } else if (hasAny(
    /(side[ -]?scroller|side[ -]?scrolling|side view|side-view|runner)/,
    /(vehicle|truck|car|bike|buggy|motorcycle|tank)/,
    /(terrain|ground traversal|hill climb|slope|road strip|terrain strip)/
  )) {
    gameType = "sidescroller_terrain";
  } else if (hasAny(
    /(platformer|platforming|run and jump|jump between platforms)/,
    /(ground traversal|terrain|ground piece|platform route|ramp)/
  )) {
    gameType = "platformer";
  }

  const detectedRoadExclusionFlag = gameType !== "other" || hasAny(
    /(road section|track segment|terrain strip|ground piece|pre-built ground|prebuilt ground)/,
    /(track layout|terrain layout|road pipeline|road\.zip)/
  );

  const merged = {
    ...existingValue,
    gameType,
    roadExclusionFlag: detectedRoadExclusionFlag,
    source: "prompt_heuristic_v5"
  };

  if (existingValue.roadPipelineUserOverride === true) {
    merged.roadExclusionFlag = existingValue.requestRoadPipeline === true;
    merged.source = "user_override_preserved_v1";
  }

  return merged;
}


function normalizeRoadPieceName(value = "") {
  return String(value || "").replace(/\\/g, "/").split("/").pop().replace(/\.obj$/i, "").trim();
}

function normalizeRoadIndex(rawIndex = null) {
  const raw = rawIndex && typeof rawIndex === "object" ? rawIndex : {};
  const normalized = {};
  const allKeys = new Set([
    ...Object.keys(FALLBACK_ROAD_INDEX || {}),
    ...Object.keys(raw || {})
  ]);

  allKeys.forEach((key) => {
    const fallbackEntry = FALLBACK_ROAD_INDEX[key] || FALLBACK_ROAD_INDEX[normalizeRoadPieceName(key)] || null;
    const rawEntry = raw[key] || raw[normalizeRoadPieceName(key)] || null;
    const merged = {
      ...(fallbackEntry || {}),
      ...(rawEntry || {}),
      dimensions: { ...((fallbackEntry && fallbackEntry.dimensions) || {}), ...((rawEntry && rawEntry.dimensions) || {}) },
      entry: { ...((fallbackEntry && fallbackEntry.entry) || {}), ...((rawEntry && rawEntry.entry) || {}) },
      exit: { ...((fallbackEntry && fallbackEntry.exit) || {}), ...((rawEntry && rawEntry.exit) || {}) }
    };
    const pieceName = normalizeRoadPieceName(merged.name || merged.filename || key);
    if (!pieceName) return;
    normalized[pieceName] = {
      ...merged,
      name: pieceName,
      filename: merged.filename || `${pieceName}.obj`,
      tags: Array.isArray(merged.tags) ? merged.tags : [],
      slots: Array.isArray(merged.slots) ? merged.slots : [],
      slot_count: Number.isFinite(Number(merged.slot_count)) ? Number(merged.slot_count) : (Array.isArray(merged.slots) ? merged.slots.length : 0),
      material_file: merged.material_file === undefined ? "" : merged.material_file
    };
  });

  return normalized;
}

async function loadRoadIndex(bucket) {
  try {
    const roadZipFile = bucket.file(ROAD_PIPELINE_ZIP_PATH);
    const [exists] = await roadZipFile.exists();
    if (!exists) {
      throw new Error(`Road.zip not found at ${ROAD_PIPELINE_ZIP_PATH}`);
    }

    const [zipBuffer] = await roadZipFile.download();
    const zip = await JSZip.loadAsync(zipBuffer);

    for (const entryName of ROAD_PIPELINE_INDEX_ENTRY_NAMES) {
      const entry = zip.file(entryName);
      if (!entry) continue;
      const parsed = JSON.parse(await entry.async('string'));
      return normalizeRoadIndex(parsed);
    }

    throw new Error(`Road.zip is missing ${ROAD_PIPELINE_INDEX_ENTRY_NAMES.join(' / ')} at zip root.`);
  } catch (err) {
    console.warn(`[ROAD] Failed to load road index from Road.zip root: ${err.message}`);
  }
  console.warn("[ROAD] Falling back to embedded road index.");
  return normalizeRoadIndex(FALLBACK_ROAD_INDEX);
}

function buildRoadPlanningContext(roadPipeline = null) {
  if (!roadPipeline?.roadExclusionFlag) return "";
  return `

ROAD PIPELINE ACTIVE:
- gameType=${roadPipeline.gameType || "other"}
- roadExclusionFlag=true
- Road.zip assets are bootstrapped into models/ by the frontend before planning, and Road_Index.json is loaded from the zip root by the proxy.
- The asset roster MUST exclude road sections, terrain strips, ground pieces, track segments, and roadside terrain filler geometry.
- The planner MUST treat ground generation as a dedicated road-first sequencer concern, not a free-form terrain-authoring problem.
- A deterministic Road Sequencer tranche will be injected as tranche 1 before normal execution.
- That sequencer tranche MUST use Road.zip pieces as the primary road-building blocks and MUST also add Cherry3D primitive terrain as complimentary fill around the placed road layout.
- Complimentary terrain fill may use ONLY hidden .primitives keys 4-14 and must NEVER use deprecated model primitive keys 17, 18, 21, 34, or 35.
- Primitive terrain fill must be adjacent to, connected with, and elevation-matched to the assembled road sections. It must never replace a road piece that already covers that role.
- Do NOT spend extra tranches re-authoring the drivable road shell with guessed meshes. Primitive work here is for stitched surrounding terrain, shoulders, verges, embankments, runoff, underfill, and neighboring ground continuity only.
- All 2D UI / HUD / bars / menus / flat interface surfaces belong in models/23 only, never in models/2 or objects3d.
`;
}

function buildRoadSequencerPrompt(roadPipeline = null, roadIndex = {}) {
  if (!roadPipeline?.roadExclusionFlag) return "";
  const validTypesByGame = {
    racing: "all 27 road piece types are valid",
    sidescroller_terrain: "valid piece types: straight, bump, ramp, start. Exclude: corners, hairpin, curved, pit, bridge, split, skew.",
    platformer: "valid piece types: straight, bump, ramp. Exclude: corners, curved, pit, bridge, split, skew.",
    other: "valid piece types: straight, bump, ramp, start."
  };
  return `ROAD SEQUENCER — MANDATORY FIRST TRANCHE
You are implementing the deterministic road / terrain sequencer directly into models/2.

ROAD PIPELINE SETTINGS:
- gameType: ${roadPipeline.gameType || "other"}
- roadExclusionFlag: true
- targetLength heuristic: short=120u | medium=200u | long=350u
- validPieces: ${validTypesByGame[roadPipeline.gameType] || validTypesByGame.other}

NON-NEGOTIABLE RULES:
1. Work ONLY in models/2.
2. Treat Road.zip OBJ pieces as already present in models/ and already registered by assets.json.
3. Road.zip pieces are the PRIMARY building blocks for road design and assembly. Use Cherry3D primitives only as complimentary terrain fill beyond the authored road pieces.
4. DO NOT create or replace a road, track, lane surface, or authored path section from primitives when a Road.zip piece covers that role.
5. Apply RGB slot colours from Road_Index.json after createObject() and before safeAddObject().
6. material_file must be '' for every road slot without exception.
7. The sequencer must validate joins before placement. If a candidate join is invalid, reject and rebuild the sequence rather than patching a broken join.
8. Insert roadStartPositions first for racing / road-start games unless the prompt clearly requires a different start piece.
9. After each road piece placement, add or extend complimentary primitive terrain so the surrounding ground remains visually connected to the road layout.
10. Complimentary terrain fill may use ONLY hidden .primitives keys 4-14. Deprecated model primitive keys 17, 18, 21, 34, and 35 are forbidden.
11. Primitive terrain must be adjacent to, connected with, and elevation-matched to the assembled road sections. No visible seams, floating filler blocks, or disconnected terrain islands are allowed beside the road.
12. Primitive terrain fill should cover shoulders, verge strips, runoff, roadside pads, embankment support, underfill below elevated road segments, and any neighboring ground continuity the scene needs after the Road.zip layout is assembled.
13. Later tranches should inherit the sequenced road + stitched terrain layout as ground truth.
14. CURSOR TRACKING — maintain three cursors across the entire sequence:
   - z_cursor: advances by piece.dimensions.length after each placement (world Z position).
   - x_cursor: advances by piece.exit.x_drift after each placement (lateral offset from baseline).
   - y_cursor: advances by piece.exit.y_rise after each placement (elevation change).
   Port validation uses x_cursor: next.entry.x_left must equal current.exit.x_left + x_cursor (tolerance 0.15u).
   y_cursor must return within 0.5u of 0 before the track ends — insert a compensating ramp if needed.
   x_cursor must return to 0 at track end — insert a compensating skew or curved piece if needed.
15. CORNER ROTATION — corner_exit pieces (tagged corner_exit in road_index) require a rotateY value
    computed from the piece's exit.face field:
      exit.face = 'x-'  →  rotateY = 90°
      exit.face = 'x+'  →  rotateY = -90°
    Apply this rotation to the placed object immediately after position assignment and before safeAddObject.
    Non-corner-exit pieces use rotateY = 0 (default, no rotation needed).

REQUIRED CODE SHAPE:
- Add a ROAD_INDEX / roadIndex constant or helper derived from the data below.
- Add an applyRoadSlotColours(obj, pieceName) helper that loops the exact slot data for that piece.
- Add an emitAdjacentPrimitiveTerrain(...) helper (or equivalent) that builds primitive roadside ground using only .primitives keys 4-14.
- Add a sequencer helper that:
  — chooses pieces based on game type and prompt keywords
  — maintains z_cursor, x_cursor, y_cursor across the full sequence (see Rule 14)
  — validates adjacent port joins using x_cursor before each placement (tolerance 0.15u)
  — rejects and rebuilds the entire sequence on any port mismatch — never patches
  — applies rotateY from exit.face for corner_exit pieces (see Rule 15)
  — emits connected primitive terrain immediately after or alongside each accepted road placement
  — ensures the primitive terrain touches the road edges and inherits the correct local elevation / slope context
  — compensates open x_cursor or y_cursor at track end before finalising
- Primitive terrain helpers must never replace an authored road segment; they only fill the remaining neighboring terrain volume.
- For fixed tracks, emit the validated placements immediately in the world-build path.
- For dynamic tracks, emit a self-contained generator function that still validates ports before adding objects.

CANONICAL SLOT-COLOUR APPLICATION:
function applyRoadSlotColours(obj, pieceName) {
  const slotMap = roadIndex[pieceName].slots;
  for (let i = 0; i < slotMap.length; i++) {
    const s = slotMap[i];
    obj.data[i].r = s.r / 255;
    obj.data[i].g = s.g / 255;
    obj.data[i].b = s.b / 255;
    obj.data[i].material_file = '';
  }
}

ROAD INDEX (authoritative per-piece geometry + slot colours):
${JSON.stringify(roadIndex, null, 2)}
`;
}

function injectRoadSequencerTranche(plan, roadPipeline = null) {
  if (!roadPipeline?.roadExclusionFlag) return plan;
  const existing = Array.isArray(plan?.tranches) ? plan.tranches : [];
  const alreadyPresent = existing.some(tranche => /road sequencer|road pipeline|road tranche/i.test(`${tranche?.name || ""} ${tranche?.description || ""} ${tranche?.prompt || ""}`));
  if (alreadyPresent) return plan;

  const roadTranche = {
    kind: "road_sequencer",
    name: "Road Sequencer Ground Layout",
    description: "Deterministic road-first layout from Road.zip + Road_Index.json (zip root), plus stitched Cherry3D primitive terrain fill around the placed road shell before other gameplay systems build on top.",
    anchorSections: ["Road Pipeline"],
    purpose: "Create a geometrically valid road or terrain layout first so later tranches inherit a known ground truth.",
    systemsTouched: ["world_build", "road_pipeline", "ground_layout"],
    filesTouched: ["models/2"],
    visibleResult: "A placed and colour-applied Road.zip layout exists first, with adjacent Cherry3D primitive terrain stitched around it before later tranches.",
    safetyChecks: [
      "road slots use direct RGB colouring only",
      "material_file is empty string on every road slot",
      "adjacent piece ports validate before placement"
    ],
    expectedFiles: ["models/2"],
    dependencies: [],
    expertAgents: ["world_authoring", "physics_simulation"],
    phase: 0,
    qualityCriteria: [
      "port-validated layout",
      "piece-specific slot colours",
      "primitive terrain stitched adjacent to road edges",
      "later tranches inherit stable ground truth"
    ],
    prompt: "__ROAD_SEQUENCER_PROMPT__"
  };

  return {
    ...plan,
    analysis: `${String(plan?.analysis || "").trim()}\n\nRoad pipeline active — a deterministic road sequencer tranche has been inserted ahead of the planner's normal tranche sequence.`.trim(),
    tranches: [roadTranche, ...existing]
  };
}


/* ─── SCAFFOLD + SDK INSTRUCTION BUNDLE: fetched from Firebase ───
   All project-level instruction files live under:
     ${projectPath}/ai_system_instructions/

   We classify them into:
   - scaffold: immutable game foundation / structural rules
   - sdk: engine reference / API facts / certainty fallback
   - other: additional instruction docs (treated as sdk-side supplemental context)
*/

function classifyInstructionFile(fileName = "", content = "") {
  const lowerName = String(fileName || "").toLowerCase();
  const lowerContent = String(content || "").toLowerCase();

  // SDK check runs FIRST — filename is unambiguous and must not be overridden
  // by the content-based scaffold check (SDK docs often mention "scaffold" and "immutable")
  if (
    lowerName.includes("engine_reference") ||
    lowerName.includes("engine-reference") ||
    lowerName.includes("engine reference") ||
    lowerName.includes("sdk") ||
    lowerContent.includes("cherry3d engine reference") ||
    lowerContent.includes("platform invariants")
  ) {
    return "sdk";
  }

  if (
    lowerName.includes("scaffold") ||
    lowerName.includes("binding_law") ||
    lowerName.includes("binding-law") ||
    lowerName.includes("case_law") ||
    lowerName.includes("case-law") ||
    lowerName.includes("pattern_library") ||
    lowerName.includes("pattern-library") ||
    lowerContent.includes("working shipped games are the law") ||
    lowerContent.includes("binding law + pattern library") ||
    (lowerContent.includes("scaffold") && lowerContent.includes("immutable"))
  ) {
    return "scaffold";
  }

  if (
    lowerName.includes("reference_game_patterns") ||
    lowerName.includes("reference-game-patterns") ||
    lowerName.includes("reference pack") ||
    lowerName.includes("reference_pack") ||
    lowerName.includes("reference-pack") ||
    lowerName.includes("pattern pack") ||
    lowerName.includes("pattern_pack") ||
    lowerName.includes("pattern-pack") ||
    lowerContent.includes("mandatory structural patterns") ||
    lowerContent.includes("preferred product/polish patterns")
  ) {
    return "reference_patterns";
  }

  return "other";
}



function getInstructionBundleSortRank(fileName = "", kind = "other") {
  const lowerName = String(fileName || "").toLowerCase();
  if (kind === "scaffold") {
    if (lowerName.includes("combined")) return 0;
    if (lowerName.includes("overlay") || lowerName.includes("patch")) return 1;
    return 2;
  }
  if (kind === "sdk") return 10;
  if (kind === "reference_patterns") return 20;
  return 30;
}

function buildScaffoldSelectionContext(scaffoldSelection = null) {
  if (!scaffoldSelection || typeof scaffoldSelection !== "object") return "";
  const signalSummary = scaffoldSelection.signalSummary || {};
  const sectors = signalSummary.sectors || {};
  const reasons = Array.isArray(scaffoldSelection.checklist)
    ? scaffoldSelection.checklist.slice(0, 8).map((item, index) =>
        `${index + 1}. ${item.label} (${item.delta >= 0 ? '+' : ''}${item.delta})${item.evidence ? ` — ${item.evidence}` : ''}`
      ).join("\n")
    : "";
  const candidates = Array.isArray(scaffoldSelection.candidateOverlayScores)
    ? scaffoldSelection.candidateOverlayScores.slice(0, 5).map((item, index) =>
        `${index + 1}. ${item.overlayFamilyLabel || item.overlayFamilyId || item.descriptor} | total=${item.score} | overlayCompatibility=${item.compatibilityScore}${item.hasCombined ? ' | combined=yes' : ' | combined=no'}${item.hasOverlay ? ' | overlay=yes' : ' | overlay=no'}`
      ).join("\n")
    : "";
  const promptTags = Array.isArray(scaffoldSelection.promptRequirementProfile?.positiveTags)
    ? scaffoldSelection.promptRequirementProfile.positiveTags.slice(0, 16).join(", ")
    : "";
  return `

SELECTED SCAFFOLD ARTIFACT:
- familyId: ${scaffoldSelection.familyId || "generic_universal"}
- familyLabel: ${scaffoldSelection.familyLabel || "Universal Core Scaffold"}
- familyVersion: ${scaffoldSelection.familyVersion || "1"}
- confidence: ${scaffoldSelection.confidence || 35}/99
- scoreMarginVsRunnerUp: ${scaffoldSelection.margin || 0}
- overlayCompatibilityScore: ${scaffoldSelection.overlayCompatibilityScore || 0}
- selectedSignature: ${scaffoldSelection.signature || "unknown"}
- sourceCombinedScaffoldPath: ${scaffoldSelection.sourceCombinedScaffoldPath || scaffoldSelection.sourceModel2Path || "(not provided)"}
- sourceOverlayScaffoldPath: ${scaffoldSelection.sourceOverlayScaffoldPath || "(none)"}
- sourceZipArchivePath: ${scaffoldSelection.sourceZipArchivePath || "(none)"}
- selectedZipGroup: ${scaffoldSelection.selectedZipGroupDescriptor || scaffoldSelection.selectedZipGroupKey || "(none)"}
- resolvedModel2Path: ${scaffoldSelection.resolvedModel2Path || "(not provided)"}
- resolvedInstructionFolder: ${scaffoldSelection.resolvedInstructionFolder || "(default project ai_system_instructions)"}
- cameraSector: ${sectors.cameraSector || "ambiguous"}
- controllerSector: ${sectors.controllerSector || "ambiguous"}
- worldSector: ${sectors.worldSector || "ambiguous"}
- interactionSector: ${sectors.interactionSector || "ambiguous"}
- promptRequirementTags: ${promptTags || "(none)"}

OVERLAY CANDIDATE SCOREBOARD:
${candidates || "1. No overlay candidate scoreboard provided."}

SELECTOR RULES:
- The system must compare the Master Game Prompt requirements against every available GAME OVERLAY scaffold candidate it can fetch from the scaffold zip / instruction folders.
- The winning overlay chooses the paired COMBINED scaffold from the same set.
- models/2 is materialized from the selected COMBINED scaffold file stored under AI System Instructions.
- The matching GAME OVERLAY scaffold file is injected beside the combined scaffold in the selected instruction bundle.
- Do not drift to sibling scaffold families unless the selected scaffold text itself explicitly authorises it.
- Foundation-A extends the selected scaffold artifact. It does NOT regenerate scaffold architecture.

TOP SELECTOR REASONS:
${reasons || "1. No detailed selector checklist provided."}
`;
}

async function fetchInstructionBundle(bucket, projectPath, folderOverride = null) {
  const candidateFolders = [];
  if (folderOverride && typeof folderOverride === "string") candidateFolders.push(folderOverride);
  candidateFolders.push(`${projectPath}/ai_system_instructions`);

  let lastEmptyFolder = candidateFolders[0];
  for (const folder of candidateFolders.filter((value, index, arr) => value && arr.indexOf(value) === index)) {
    lastEmptyFolder = folder;
    try {
      const [files] = await bucket.getFiles({ prefix: folder + "/" });
      if (!files || files.length === 0) {
        console.warn(`fetchInstructionBundle: no files found at ${folder}/`);
        continue;
      }

      files.sort((a, b) => a.name.localeCompare(b.name));
      const parts = await Promise.all(
        files.map(async (file) => {
          const [fileContent] = await file.download();
          const content = fileContent.toString("utf8");
          const fileName = file.name.split("/").pop();
          return {
            fileName,
            content,
            kind: classifyInstructionFile(fileName, content)
          };
        })
      );
      parts.sort((a, b) => {
        const rankDelta = getInstructionBundleSortRank(a.fileName, a.kind) - getInstructionBundleSortRank(b.fileName, b.kind);
        if (rankDelta !== 0) return rankDelta;
        return a.fileName.localeCompare(b.fileName);
      });

      const scaffoldDocs = parts.filter(p => p.kind === "scaffold");
      const sdkDocs = parts.filter(p => p.kind === "sdk");
      const referencePatternDocs = parts.filter(p => p.kind === "reference_patterns");
      const otherDocs = parts.filter(p => p.kind === "other");

      const formatDocs = (docs) => docs.map(doc =>
        `--- ${doc.fileName} ---\n${doc.content}`
      ).join("\n\n");

      const scaffoldText = formatDocs(scaffoldDocs);
      const sdkText = formatDocs([...sdkDocs, ...otherDocs]);
      const referencePatternsText = formatDocs(referencePatternDocs);

      const sections = [];
      if (scaffoldText) {
        sections.push(`=== BINDING CHERRY3D SCAFFOLD LAW ===\n${scaffoldText}`);
      }
      if (sdkText) {
        sections.push(`=== SUBORDINATE CHERRY3D SDK / ENGINE NOTES ===\n${sdkText}`);
      }

      const combinedText = sections.join("\n\n");
      console.log(
        `fetchInstructionBundle: loaded ${files.length} file(s) from ${folder} ` +
        `(scaffold=${scaffoldDocs.length}, sdk=${sdkDocs.length}, reference=${referencePatternDocs.length}, other=${otherDocs.length})`
      );

      return {
        scaffoldText,
        sdkText,
        referencePatternsText,
        combinedText,
        scaffoldCount: scaffoldDocs.length,
        sdkCount: sdkDocs.length,
        referencePatternCount: referencePatternDocs.length,
        otherCount: otherDocs.length,
        resolvedFolder: folder
      };
    } catch (err) {
      console.error(`fetchInstructionBundle failed for ${folder}:`, err.message);
    }
  }

  return {
    scaffoldText: "",
    sdkText: "",
    referencePatternsText: "",
    combinedText: "",
    scaffoldCount: 0,
    sdkCount: 0,
    referencePatternCount: 0,
    otherCount: 0,
    resolvedFolder: lastEmptyFolder
  };
}


function assertInstructionBundle(bundle, phaseLabel = "Pipeline") {
  if (!bundle?.scaffoldText) {
    throw new Error(`${phaseLabel}: binding Scaffold law missing from ai_system_instructions/.`);
  }
  if (!bundle?.sdkText) {
    throw new Error(`${phaseLabel}: subordinate SDK / Engine Notes missing from ai_system_instructions/.`);
  }
}


function splitReferencePatternSections(referencePatternsText = "") {
  const text = String(referencePatternsText || "").trim();
  if (!text) return { structural: "", polish: "", full: "" };
  const structuralMatch = text.match(/=== MANDATORY STRUCTURAL PATTERNS[\s\S]*?(?=\n=== PREFERRED PRODUCT\/POLISH PATTERNS|$)/i);
  const polishMatch = text.match(/=== PREFERRED PRODUCT\/POLISH PATTERNS[\s\S]*?(?=\n=== TRANCHE ROUTING GUIDE|$)/i);
  return {
    structural: structuralMatch ? structuralMatch[0].trim() : "",
    polish: polishMatch ? polishMatch[0].trim() : "",
    full: text
  };
}

function buildPlanningReferencePatternContext(referencePatternsText = "") {
  const sections = splitReferencePatternSections(referencePatternsText);
  if (!sections.full) return "";
  return `

=== SECONDARY REFERENCE GAME PATTERNS ===
${sections.full}

REFERENCE PACK PRIORITY:
- Scaffold law remains primary.
- Use MANDATORY STRUCTURAL PATTERNS to shape lifecycle, pooling, overlay ownership, persistence wiring, teardown, and helper layout when the scaffold is silent or when a lawful pattern choice is needed.
- Use PREFERRED PRODUCT/POLISH PATTERNS to shape HUD, modal flow, progression flow, authored subsystem packaging, and finished-product cohesion after structural safety is satisfied.
- Never let polish preferences override scaffold compliance or structural safety.`;
}

function buildExecutionReferencePatternContext(referencePatternsText = "", tranche = null) {
  const sections = splitReferencePatternSections(referencePatternsText);
  if (!sections.full) return "";
  const kind = String(tranche?.kind || "").toLowerCase();
  const name = String(tranche?.name || "").toLowerCase();
  const purpose = String(tranche?.purpose || "").toLowerCase();
  const haystack = `${kind} ${name} ${purpose}`;
  const includePolish = /(ui|hud|modal|progression|result|pause|fail|shop|feedback|polish|flow|presentation)/.test(haystack);
  const includeStructural = includePolish ? /(foundation|scene|shell|controls|control|physics|build|core|hardening|rules|bot|input|persistence|overlay|ui_logic)/.test(haystack) : true;
  const chosen = [];
  if (includeStructural && sections.structural) chosen.push(sections.structural);
  if (includePolish && sections.polish) chosen.push(sections.polish);
  if (!chosen.length && sections.structural) chosen.push(sections.structural);
  return chosen.length
    ? `=== RELEVANT REFERENCE GAME PATTERNS ===
${chosen.join("\n\n")}

REFERENCE PACK PRIORITY:
- Scaffold law still wins.
- Structural patterns are mandatory when this tranche touches lifecycle, pooling, overlay ownership, persistence wiring, teardown, or helper layout.
- Product/polish patterns are preferences for game flow and finished-product cohesion only after structural safety is preserved.

`
    : "";
}

function flattenAssetsManifestEntries(entries) {
  const flat = [];
  for (const entry of Array.isArray(entries) ? entries : []) {
    if (!entry || typeof entry !== "object") continue;
    flat.push(entry);
    if (Array.isArray(entry.children) && entry.children.length > 0) {
      flat.push(...flattenAssetsManifestEntries(entry.children));
    }
  }
  return flat;
}
const FOUNDATION_B_DUPLICATE_REGEX = /\bfoundation[\s_-]*b\b/i;
const FOUNDATION_A_REGEX = /\bfoundation[\s_-]*a\b/i;
const PARTICLE_TRANCHE_DETECTION_REGEX = /\b(particle(?:[\s_-]*tex[\s_-]*paths)?|particle[\s_-]*billboard|emitter(?:\s+setup)?|explosions?|smoke|sparks?|fire|debris|trails?)\b/i;

function trancheTouchesParticleSystems(tranche) {
  const name = String(tranche?.name || '');
  const prompt = String(tranche?.prompt || '');
  const systemsTouched = Array.isArray(tranche?.systemsTouched) ? tranche.systemsTouched.join(' ') : '';
  const haystack = `${name}\n${prompt}\n${systemsTouched}`;
  return PARTICLE_TRANCHE_DETECTION_REGEX.test(haystack);
}

function injectFoundationBTranche(plan, approvedRosterJson = null) {
  if (!plan || !Array.isArray(plan.tranches)) return plan;
  const particleTextures = Array.isArray(approvedRosterJson?.textureAssets)
    ? approvedRosterJson.textureAssets.filter(asset => asset?.particleEffectTarget)
    : [];
  if (particleTextures.length === 0) return plan;

  const alreadyPresent = plan.tranches.some(tranche => {
    const name = String(tranche?.name || '');
    const prompt = String(tranche?.prompt || '');
    return FOUNDATION_B_DUPLICATE_REGEX.test(name) || FOUNDATION_B_DUPLICATE_REGEX.test(prompt) || /\bparticle[\s_-]*tex[\s_-]*paths\b/i.test(prompt);
  });
  if (alreadyPresent) return plan;

  const firstParticleTrancheIndex = plan.tranches.findIndex(tranche => trancheTouchesParticleSystems(tranche));
  if (firstParticleTrancheIndex < 0) return plan;

  const lines = particleTextures.map(asset => {
    const stagedPath = asset?.stagedPath || '(missing staged path)';
    const manifestKey = asset?.manifestKey || asset?.resolvedManifestKey || '(resolve from roster block)';
    return `- ${asset.particleEffectTarget}: stagedPath=${stagedPath} | manifestKey=${manifestKey}`;
  }).join('\n');

  const tranche = {
    kind: 'foundation',
    name: 'Foundation-B',
    description: 'Populate gameState.particleTextureIds with the numeric assets.json manifest key for every approved particle effect texture before any particle template or emitter tranche.',
    purpose: 'Register approved particle effect texture manifest keys in gameState.particleTextureIds so particle template tranches can pass them as albedo_texture to registerParticleTemplate().',
    systemsTouched: ['particles', 'asset registry'],
    filesTouched: ['models/2'],
    expectedFiles: ['models/2'],
    visibleResult: 'Particle templates have deterministic numeric manifest key bindings available before registration.',
    safetyChecks: ['Only approved particleEffectTarget keys are used.', 'Every approved particle texture gets its numeric manifest key in gameState.particleTextureIds.', 'PARTICLE_TEX_PATHS is NOT declared — staged Firebase paths are never used for particle templates.'],
    qualityCriteria: ['gameState.particleTextureIds is populated for every approved particleEffectTarget with the correct numeric manifest key.'],
    dependencies: ['Foundation-A'],
    prompt: `FOUNDATION-B PARTICLE TEXTURE REGISTRY
Populate gameState.particleTextureIds in models/2 with the numeric assets.json manifest key for every approved particle effect texture. This registry is read by particle template tranches which pass the key as albedo_texture to registerParticleTemplate().

Approved particle targets:
${lines}

Rules:
1. Use the exact particleEffectTarget names shown above as object keys.
2. Populate gameState.particleTextureIds using the resolved numeric manifest keys from the approved roster block.
3. Do NOT declare PARTICLE_TEX_PATHS — staged Firebase paths are never used for particle templates.
4. Do not register particle templates in this tranche; this tranche only sets up gameState.particleTextureIds.
5. The particle template tranche uses: registerParticleTemplate({ key, assetId, albedo_texture: gameState.particleTextureIds[effectName] })`,
    phase: 1,
    expertAgents: ['api_contracts']
  };

  const insertAfterIndex = typeof plan.tranches.findLastIndex === 'function'
    ? plan.tranches.findLastIndex(tranche => FOUNDATION_A_REGEX.test(String(tranche?.name || '')))
    : (() => {
        for (let i = plan.tranches.length - 1; i >= 0; i -= 1) {
          if (FOUNDATION_A_REGEX.test(String(plan.tranches[i]?.name || ''))) return i;
        }
        return -1;
      })();
  if (insertAfterIndex >= 0) {
    plan.tranches.splice(insertAfterIndex + 1, 0, tranche);
  } else {
    plan.tranches.splice(firstParticleTrancheIndex, 0, tranche);
  }
  return plan;
}


async function loadAssetsManifestIndex(bucket, projectPath) {
  try {
    const manifestFile = bucket.file(`${projectPath}/json/assets.json`);
    const [exists] = await manifestFile.exists();
    if (!exists) return new Map();
    const [content] = await manifestFile.download();
    const parsed = JSON.parse(content.toString());
    const manifestRoot = Array.isArray(parsed)
      ? parsed
      : Object.values(parsed || {}).find(v => Array.isArray(v)) || [];
    const flat = flattenAssetsManifestEntries(manifestRoot);
    const index = new Map();
    for (const entry of flat) {
      if (!entry?.title) continue;
      index.set(String(entry.title).toLowerCase(), {
        key: entry.key != null ? String(entry.key) : "",
        type: entry.type || "",
        title: entry.title
      });
    }
    return index;
  } catch (e) {
    console.warn("[ROSTER] Could not load assets.json for manifest annotation:", e.message);
    return new Map();
  }
}

function resolveRosterRole(asset) {
  return asset?.intendedRole || asset?.intendedUsage || asset?.selectionRationale || asset?.matchedRequirement || "";
}

function formatRosterNumber(value, digits = 3) {
  const num = Number(value);
  return Number.isFinite(num) ? num.toFixed(digits) : "N/A";
}

function buildRosterObjectContract(asset, stagedMeta, manifestMeta) {
  const geometry = asset?.geometryAnalysis || stagedMeta?.geometryAnalysis || null;
  const stagedPath = stagedMeta?.stagedPath || asset?.stagedPath || "(not staged)";
  const colormapFile = asset?.colormapFile || stagedMeta?.colormapFile || null;
  const colormapPath = asset?.colormapStagedPath || stagedMeta?.colormapStagedPath || null;
  const colormapConfidence = asset?.colormapConfidence || stagedMeta?.colormapConfidence || (colormapFile ? "HIGH" : "NONE");
  const uvNote = geometry?.uvMapping?.uvWrappingNote || "NOT AVAILABLE";
  // Trust roster's pre-resolved colormapManifestKey first (set by frontend annotateApprovedRosterWithManifestKeys)
  const colormapManifestKey = asset?.colormapManifestKey || stagedMeta?.colormapManifestKey || "";
  const slotCount = Number(asset?.slotCount ?? stagedMeta?.slotCount ?? asset?.meshCount ?? stagedMeta?.meshCount ?? 0);
  const manifestKey = manifestMeta?.key ? `"${manifestMeta.key}"` : "(unresolved)";
  const sourceDoc = asset?.sourceRosterDocument || "Unknown source";

  if (!geometry) {
    return `  ┌─ [${asset.assetName}]
  │  Source:       ${sourceDoc}
  │  Role:         ${resolveRosterRole(asset)}
  │  Manifest key: ${manifestKey}
  │  Staged path:  ${stagedPath}
  │
  │  GEOMETRY CONTRACT: NOT AVAILABLE — scale and position conservatively;
  │    use scale [1,1,1] and position.y = 0 as safe defaults.
  │  TEXTURE CONTRACT:
  │    Colormap file: ${colormapFile || "NOT AVAILABLE"}
  │    Colormap path: ${colormapPath || "NOT AVAILABLE"}
  │    Colormap key:  ${colormapManifestKey || "NOT AVAILABLE"}
  │    Slot count:    ${slotCount || "NOT AVAILABLE"}
  │    Mesh count:    ${(asset?.meshCount != null ? asset.meshCount : (stagedMeta?.meshCount != null ? stagedMeta.meshCount : "NOT AVAILABLE"))}
  └─`;
  }

  const size = geometry?.geometry?.size || {};
  const placement = geometry?.placement || {};
  const scale = geometry?.scale || {};
  const origin = geometry?.origin || {};
  const scaleVec = Array.isArray(scale.suggestedGameScaleVec)
    ? `[${scale.suggestedGameScaleVec.map(v => formatRosterNumber(v, 6)).join(", ")}]`
    : "[N/A]";
  const assignmentLine = (colormapPath || colormapManifestKey)
    ? `defineMaterial('mat_<asset>', 255, 255, 255, 0.5, 0.0, '${colormapManifestKey || 'RESOLVED_COLORMAP_KEY_REQUIRED'}'); then apply that registered material key across slots 0..${Math.max(0, ((slotCount || 1) - 1))} via gameState._applyMat / slot-safe scaffold logic. material_file must contain the registered material key, never the raw staged path. Default to createInstance with a registered instance parent, but if working-game law proves per-object visual overrides do not survive instancing for that pool, use createObject consistently instead.`
    : "NOT AVAILABLE";

  return `  ┌─ [${asset.assetName}]
  │  Source:       ${sourceDoc}
  │  Role:         ${resolveRosterRole(asset)}
  │  Manifest key: ${manifestKey}
  │  Staged path:  ${stagedPath}
  │
  │  GEOMETRY CONTRACT (measured values — use these directly in code):
  │    Bounding box:   W=${formatRosterNumber(size.x)}  H=${formatRosterNumber(size.y)}  D=${formatRosterNumber(size.z)}  (authored unit: ${scale.authoredUnit || "unknown"})
  │    Origin class:   ${origin.classification || "N/A"}
  │    Dominant axis:  ${placement.dominantAxis || "N/A"}
  │    Forward hint:   ${placement.forwardHint || "N/A"}
  │
  │  PLACEMENT CONTRACT (copy these values verbatim into tranche code):
  │    position.y for floor placement:    ${formatRosterNumber(placement.floorY, 6)}
  │    position.y for vertical centering: ${formatRosterNumber(placement.centerY, 6)}
  │    position.x centering correction:   ${formatRosterNumber(placement.centerOffsetX, 6)}
  │    position.z centering correction:   ${formatRosterNumber(placement.centerOffsetZ, 6)}
  │    Suggested scale (largest dim → 1): ${formatRosterNumber(scale.suggestedGameScale, 6)}
  │    Scale vector:                      ${scaleVec}
  │    Scale warning:                     ${scale.scaleWarning || "null"}
  │
  │  TEXTURE CONTRACT:
  │    Colormap file:    ${colormapFile || "NOT AVAILABLE"}
  │    Colormap path:    ${colormapPath || "NOT AVAILABLE"}
  │    Colormap key:     ${colormapManifestKey || "NOT AVAILABLE"}
  │    Colormap conf.:   ${colormapConfidence}
  │    UV mapping:       ${uvNote}
  │    Slot count:       ${slotCount || "NOT AVAILABLE"}
  │    Mesh count:       ${(asset?.meshCount != null ? asset.meshCount : (stagedMeta?.meshCount != null ? stagedMeta.meshCount : "NOT AVAILABLE"))}
  │    Assignment:       ${assignmentLine}
  └─`;
}


function buildRosterAvatarContract(asset, stagedMeta, manifestMeta) {
  const geometry = asset?.geometryAnalysis || stagedMeta?.geometryAnalysis || null;
  const stagedPath = stagedMeta?.stagedPath || asset?.stagedPath || "(not staged)";
  const manifestKey = manifestMeta?.key ? `"${manifestMeta.key}"` : "(unresolved)";
  const clips = Array.isArray(asset?.animationClips) ? asset.animationClips : [];
  const clipList = clips.length > 0 ? clips.join(", ") : "NOT AVAILABLE";
  const texturePaths = Array.isArray(asset?.stagedTexturePaths) ? asset.stagedTexturePaths : [];
  const colormapFile = asset?.colormapFile || stagedMeta?.colormapFile || null;
  const colormapPath = asset?.colormapStagedPath || stagedMeta?.colormapStagedPath || null;
  const colormapConfidence = asset?.colormapConfidence || stagedMeta?.colormapConfidence || (colormapFile ? "HIGH" : "NONE");
  const colormapManifestKey = asset?.colormapManifestKey || stagedMeta?.colormapManifestKey || "";
  const slotCount = Number(asset?.slotCount ?? stagedMeta?.slotCount ?? asset?.meshCount ?? stagedMeta?.meshCount ?? 0);
  const avatarTextureAssignment = (colormapPath || colormapManifestKey)
    ? `Optional avatar colormap rail available — if this avatar is instantiated through the non-primitive material path, define a registered material whose albedo_texture uses ${colormapManifestKey || 'RESOLVED_COLORMAP_KEY_REQUIRED'} and apply it across slots 0..${Math.max(0, ((slotCount || 1) - 1))} via gameState._applyMat / slot-safe scaffold logic. Otherwise preserve stagedTexturePaths as the authoritative fallback.`
    : `Use stagedTexturePaths directly unless a later roster annotation resolves a colormap key.`;
  const size = geometry?.geometry?.size || {};
  const placement = geometry?.placement || {};
  const scale = geometry?.scale || {};
  const scaleVec = Array.isArray(scale.suggestedGameScaleVec)
    ? `[${scale.suggestedGameScaleVec.map(v => formatRosterNumber(v, 6)).join(", ")}]`
    : "[N/A]";

  return `  ┌─ [${asset.assetName}]
  │  Source:       ${asset?.sourceZip || "Avatars.zip"}
  │  Role:         ${asset?.avatarRole || asset?.intendedRole || asset?.matchedRequirement || "avatar"}
  │  Manifest key: ${manifestKey}
  │  Staged path:  ${stagedPath}
  │
  │  GEOMETRY CONTRACT ${geometry ? "(measured values — use these directly in code)" : ": NOT AVAILABLE — scale conservatively with [1,1,1]"}
  │    Bounding box:   W=${formatRosterNumber(size.x)}  H=${formatRosterNumber(size.y)}  D=${formatRosterNumber(size.z)}
  │    position.y for floor placement:    ${formatRosterNumber(placement.floorY, 6)}
  │    position.x centering correction:   ${formatRosterNumber(placement.centerOffsetX, 6)}
  │    position.z centering correction:   ${formatRosterNumber(placement.centerOffsetZ, 6)}
  │    Suggested scale (largest dim → 1): ${formatRosterNumber(scale.suggestedGameScale, 6)}
  │    Scale vector:                      ${scaleVec}
  │
  │  ANIMATION CONTRACT:
  │    Clips:         ${clipList}
  │    Coverage:      ${asset?.animationCoverage || "NOT AVAILABLE"}
  │    Manifest file: ${asset?.stagedAnimationManifestPath || asset?.animationManifestPath || "NOT AVAILABLE"}
  │
  │  TEXTURE CONTRACT:
  │    Staged textures: ${texturePaths.length > 0 ? texturePaths.join(", ") : "NOT AVAILABLE"}
  │    Colormap file:   ${colormapFile || "NOT AVAILABLE"}
  │    Colormap path:   ${colormapPath || "NOT AVAILABLE"}
  │    Colormap key:    ${colormapManifestKey || "NOT AVAILABLE"}
  │    Colormap conf.:  ${colormapConfidence}
  │    Slot count:      ${slotCount || "NOT AVAILABLE"}
  │    Assignment:      ${avatarTextureAssignment}
  └─`;
}

async function loadApprovedRosterJson(bucket, projectPath) {
  const rosterFile = bucket.file(`${projectPath}/ai_asset_roster_approved.json`);
  const [exists] = await rosterFile.exists();
  if (!exists) return null;
  const [content] = await rosterFile.download();
  const parsed = JSON.parse(content.toString());
  if (!parsed?._meta?.approved) return null;
  return parsed;
}

/* ── Load approved Asset Roster from Firebase (if present) ──────
   Returns a formatted context block string, or empty string if no
   roster was approved for this run.                               */
async function loadApprovedRosterBlock(bucket, projectPath) {
  try {
    const r = await loadApprovedRosterJson(bucket, projectPath);
    if (!r) return "";

    const manifestIndex = await loadAssetsManifestIndex(bucket, projectPath);
    const stagedIndex = new Map(
      (r.stagedAssets || [])
        .filter(a => a?.assetName)
        .map(a => [String(a.assetName).toLowerCase(), a])
    );

    // ── Manifest key resolution strategy ────────────────────────────────────
    // Priority 1: asset.manifestKey already on the roster — set by the frontend's
    //             annotateApprovedRosterWithManifestKeys() after syncAssetsJson().
    //             This is the most reliable source: the frontend did the lookup
    //             at the right moment (after copyRosterAssetsToModels + syncAssetsJson),
    //             resolved the numeric key, and saved it back to ai_asset_roster_approved.json.
    // Priority 2: Re-resolve from assets.json via manifestIndex — fallback only,
    //             used when manifestKey is missing or empty on the roster asset.
    // Never fall through to asset name strings — that is what causes asset names
    // instead of numeric key #s to appear in the executor output.
    function resolveManifestMeta(asset, manifestIndex) {
      const preResolved = asset?.manifestKey ? String(asset.manifestKey) : "";
      if (preResolved) return { key: preResolved, type: asset?.manifestLocation === "models-folder-child" ? "object" : "other", _source: "roster" };
      // Fallback: re-derive from assets.json index
      const copiedName = String(asset?.copiedModelFilename || "").toLowerCase();
      const assetName  = String(asset?.assetName || "").toLowerCase();
      return manifestIndex.get(copiedName) || manifestIndex.get(assetName) || null;
    }

    function resolveColormapManifestMeta(asset, manifestIndex) {
      const preResolved = asset?.colormapManifestKey ? String(asset.colormapManifestKey) : "";
      if (preResolved) return { key: preResolved, _source: "roster" };
      const colormapFile = String(asset?.colormapFile || "").toLowerCase();
      return colormapFile ? (manifestIndex.get(colormapFile) || null) : null;
    }

    const objs = (r.objects3d || []).map(a => {
      const manifestMeta = resolveManifestMeta(a, manifestIndex);
      const stagedMeta   = stagedIndex.get(String(a.assetName || "").toLowerCase());
      if (!manifestMeta?.key) {
        console.warn(`[ROSTER] objects3d asset "${a.assetName}" has no resolved manifest key — check assets.json sync and annotateApprovedRosterWithManifestKeys timing.`);
      }
      return buildRosterObjectContract(a, stagedMeta, manifestMeta);
    }).join("\n");

    const avatars = (r.avatars || []).map(a => {
      const manifestMeta = resolveManifestMeta(a, manifestIndex);
      const stagedMeta   = stagedIndex.get(String(a.assetName || "").toLowerCase());
      if (!manifestMeta?.key) {
        console.warn(`[ROSTER] avatars asset "${a.assetName}" has no resolved manifest key — check assets.json sync and annotateApprovedRosterWithManifestKeys timing.`);
      }
      return buildRosterAvatarContract(a, stagedMeta, manifestMeta);
    }).join("\n");

    const particleTextures = (r.textureAssets || []).filter(a => a.particleEffectTarget);
    const texsParticle = particleTextures.map(a => {
      const manifestMeta = resolveManifestMeta(a, manifestIndex);
      if (!manifestMeta?.key) {
        console.warn(`[ROSTER] particle texture "${a.assetName}" has no resolved manifest key.`);
      }
      return `  - ${a.assetName} (from ${a.sourceRosterDocument}) → particleEffectTarget: "${a.particleEffectTarget}" | ${resolveRosterRole(a)} | staged path: ${a.stagedPath || "(not staged)"} | manifest key: ${manifestMeta?.key ? `"${manifestMeta.key}"` : "(unresolved — check sync timing)"}`;
    }).join("\n");

    const staged = (r.stagedAssets || []).map(a => {
      const manifestMeta = resolveManifestMeta(a, manifestIndex);
      const colormapSuffix = a.colormapStagedPath ? ` | colormap: ${a.colormapStagedPath}` : "";
      const textureSuffix  = Array.isArray(a.stagedTexturePaths) && a.stagedTexturePaths.length > 0 ? ` | avatar textures: ${a.stagedTexturePaths.join(", ")}` : "";
      const animSuffix     = a.stagedAnimationManifestPath ? ` | animations: ${a.stagedAnimationManifestPath}` : "";
      return `  - ${a.copiedModelFilename || a.assetName} → ${a.stagedPath}${manifestMeta?.key ? ` | manifest key: "${manifestMeta.key}"` : " | manifest key: (unresolved — check sync timing)"}${colormapSuffix}${textureSuffix}${animSuffix}`;
    }).join("\n");
    const vn = r.visualDirectionNotes || {};
    const sf = r._meta?.stagedFolder || "";

    return `

═══════════════════════════════════════════════════════════
APPROVED GAME-SPECIFIC ASSET ROSTER — FIRST-CLASS COMPANION DOCUMENT
Authority equal to the Master Prompt and all reference images.
All tranche planning and execution MUST use these approved assets.
═══════════════════════════════════════════════════════════

GAME INTERPRETATION:
${r.gameInterpretationSummary || ""}

APPROVED 3D OBJECTS (${(r.objects3d||[]).length}):
${objs || "  (none)"}

APPROVED AVATARS (${(r.avatars||[]).length}):
${avatars || "  (none)"}

APPROVED PARTICLE EFFECT TEXTURES (${particleTextures.length}) — Foundation-B MUST populate gameState.particleTextureIds with the numeric manifest key for each effect. Particle template tranches pass that key as albedo_texture to registerParticleTemplate(). PARTICLE_TEX_PATHS is NOT used:
${texsParticle || "  (none)"}

STAGED ASSET FOLDER: ${sf}
STAGED FILES (Firebase paths — use these in models/2 and models/23):
${staged || "  (none extracted)"}

ASSETS.JSON MANIFEST LOCATIONS (after frontend copy + sync):
- Approved 3D objects and approved avatars register as children of the Models folder, key "15".
- Approved particle textures register at root level with their own assigned numeric keys.
- The per-asset manifest keys are resolved above — use those exact keys for all asset references in models/2 and models/23.

VISUAL DIRECTION:
  Color Direction:    ${vn.colorDirection || "N/A"}
  Material Style:     ${vn.materialStyle || "N/A"}
  Realism Level:      ${vn.realismLevel || "N/A"}
  Environmental Tone: ${vn.environmentalTone || "N/A"}
  Surface Treatment:  ${vn.surfaceTreatment || "N/A"}
  FX Relevance:       ${vn.fxRelevance || "N/A"}

TRANCHE DESIGN & EXECUTION REQUIREMENT:
1. Tranche Design MUST plan explicitly around these approved assets.
2. Every tranche touching rendered content, obstacles, environment, scene objects, playable characters, or enemies MUST incorporate the relevant approved assets from this roster.
3. Visual Direction notes above govern color, material, and FX treatment throughout all tranches.
4. Reference staged files by their Firebase staged paths or assets.json keys.
5. Color direction and surface treatment must be consistent throughout all tranches.
6. PARTICLE TEXTURE REGISTRY: A Foundation-B sub-tranche MUST be planned immediately after Foundation-A. Its only job is to populate gameState.particleTextureIds keyed by particleEffectTarget using the exact numeric assets.json manifest keys from the Approved Asset Roster block. PARTICLE_TEX_PATHS is NOT declared. Staged Firebase paths are NOT used for particle templates.
7. Every approved particle effect texture MUST be applied at the particle template level using the numeric manifest key as albedo_texture: registerParticleTemplate({ key, assetId, albedo_texture: gameState.particleTextureIds[effectName] }). Do NOT use extraData: { material_file: PARTICLE_TEX_PATHS[...] } — that pattern is forbidden. Populating gameState.particleTextureIds is necessary but only useful if albedo_texture is also passed to registerParticleTemplate.
8. 3D OBJECT / AVATAR REGISTRY: Every tranche touching visible scene content MUST branch cleanly between the eleven Cherry3D system primitives (cube, square, plane, sphere, cylinder, capsule, cone, torus, torusknot, tetrahedron, icosahedron) and non-primitive approved roster assets. If the object is intentionally one of those eleven primitives, skip external scan/roster geometry-texture-slotCount enforcement for that object and use primitive-safe logic only. For every other visible gameplay object, avatar, or enemy, you MUST use an approved roster asset via gameState.objectids and the resolved assets.json manifest keys surfaced above.
9. GEOMETRY CONTRACTS surfaced above are arithmetic, not suggestions. When present, copy the exact placement and scale values into planning and execution prompts without paraphrase.
10. Avatar tranches must name the exact approved avatar asset and, when available, the required animation clip names from the ANIMATION CONTRACT block. If an avatar contract also surfaces a colormap key/path, treat it as additive texture-contract evidence rather than ignoring it.
11. TEXTURE CONTRACTS surfaced above are mandatory for non-primitive roster 3D objects and for avatar-path assets whenever a colormap key/path is surfaced. Use the scaffold material-registry path: define a registered material whose albedo_texture is the resolved numeric colormap manifest key from assets.json, then apply that material key across all valid slots using gameState._applyMat / registerObjectContract-safe logic. material_file must hold the registered material key, never a raw staged path. When no colormap key/path is surfaced, stagedTexturePaths remain the fallback texture rail.
12. SLOT CONTRACT: slotCount is the primary hard loop bound for material application (fallback to meshCount only if slotCount is unavailable).
13. TEXTURED INSTANCE PATH: For textured non-primitive scene objects, register an instance parent and use createInstance as the default path. If working-game law proves that per-object visual overrides do not survive instancing for that pool, use createObject consistently for that pool instead of mixing createInstance and createObject.
═══════════════════════════════════════════════════════════`;
  } catch (e) {
    console.warn("[ROSTER] Could not load approved roster:", e.message);
    return "";
  }
}


/* ── DYNAMIC_ARCHITECTURE_JSON_SCHEMA — REMOVED ─────────────
   Architect pass has been merged into single-pass planner.
   No intermediate architecture spec is generated. ────────── */



/* ── helper: call Claude API ─────────────────────────────────── */
const CLAUDE_OVERLOAD_MAX_RETRIES = 5;
const CLAUDE_OVERLOAD_BASE_DELAY_MS = 1250;
const CLAUDE_OVERLOAD_MAX_DELAY_MS = 12000;

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

function computeClaudeRetryDelayMs(attempt) {
  const exponentialDelay = Math.min(
    CLAUDE_OVERLOAD_BASE_DELAY_MS * Math.pow(2, Math.max(0, attempt - 1)),
    CLAUDE_OVERLOAD_MAX_DELAY_MS
  );
  const jitter = Math.floor(Math.random() * 700);
  return exponentialDelay + jitter;
}

function isClaudeOverloadError(status, message = "") {
  const normalized = String(message || "").toLowerCase();
  if ([429, 500, 502, 503, 504, 529].includes(Number(status))) return true;
  // Network-level transient failures — no HTTP status code, matched by message
  if (
    normalized.includes("econnreset")     ||
    normalized.includes("econnrefused")   ||
    normalized.includes("etimedout")      ||
    normalized.includes("enotfound")      ||
    normalized.includes("socket hang up") ||
    normalized.includes("network error")  ||
    normalized.includes("fetch failed")
  ) return true;
  return (
    normalized.includes("overloaded")            ||
    normalized.includes("overload")              ||
    normalized.includes("rate limit")            ||
    normalized.includes("too many requests")     ||
    normalized.includes("capacity")              ||
    normalized.includes("temporarily unavailable")
  );
}

/* ── buildSystemBlocks ───────────────────────────────────────────────────
   Converts a system prompt into the structured block array that the
   Anthropic API needs for prompt caching.

   If `system` is already an array of blocks (from callers that build
   their own structure), it is passed through unchanged.

   If `system` is a plain string it is split on a sentinel comment so
   that the large static scaffold section gets its own cacheable block:

     "<scaffold text>  <!-- CACHE_BREAK -->  <dynamic text>"
                                ↑
              cache_control: ephemeral goes on the block that ends HERE

   Everything before CACHE_BREAK is the scaffold (static per project,
   identical across all tranche calls → Anthropic caches it).
   Everything after is the dynamic planning rules / output format
   instructions (changes per call → not cached).

   If there is no CACHE_BREAK sentinel the whole string is sent as a
   single text block with cache_control on it — still beneficial when
   the system prompt is identical across calls (e.g. spec-validation
   calls that share the same system string).
   ─────────────────────────────────────────────────────────────────── */
function buildSystemBlocks(system) {
  if (Array.isArray(system)) return system;          // already structured
  if (!system) return [];

  const SENTINEL = "<!-- CACHE_BREAK -->";
  const breakIdx = system.indexOf(SENTINEL);

  if (breakIdx >= 0) {
    const staticPart  = system.slice(0, breakIdx).trimEnd();
    const dynamicPart = system.slice(breakIdx + SENTINEL.length).trimStart();
    const blocks = [];
    if (staticPart) {
      blocks.push({
        type: "text",
        text: staticPart,
        cache_control: { type: "ephemeral" }    // ← cache the scaffold here
      });
    }
    if (dynamicPart) {
      blocks.push({ type: "text", text: dynamicPart });  // dynamic — not cached
    }
    return blocks;
  }

  // No sentinel — cache the whole system prompt as one block
  return [{ type: "text", text: system, cache_control: { type: "ephemeral" } }];
}

async function callClaude(apiKey, { model, maxTokens, system, userContent, effort, budgetTokens, useThinking = true }) {
  const systemBlocks = buildSystemBlocks(system);

  const body = {
    model,
    max_tokens: maxTokens,
    system: systemBlocks,
    messages: [{ role: "user", content: userContent }]
  };

  const headers = {
    "Content-Type": "application/json",
    "x-api-key": apiKey,
    "anthropic-version": "2023-06-01",
    "anthropic-beta": "prompt-caching-2024-07-31"   // ← enable prompt caching
  };

  const isOpus47 = model && model.startsWith('claude-opus-4-7');

  if (isOpus47) {
    // Opus 4.7: adaptive thinking only — budget_tokens returns 400 error
    // temperature/top_p/top_k also return 400 on 4.7, omitted entirely
    // useThinking=false omits the thinking param entirely, disabling thinking
    // for execution tranches that don't need deep reasoning.
    if (useThinking) {
      body.thinking = { type: 'adaptive' };
    }
    if (effort) {
      body.output_config = { effort };
    }
  } else {
    // Opus 4.6 / Sonnet 4.6: legacy budget_tokens path
    if (budgetTokens) {
      body.thinking = { type: 'enabled', budget_tokens: budgetTokens };
    }
    if (effort) {
      body.output_config = { effort };
    }
  }

  let lastError = null;

  for (let attempt = 1; attempt <= CLAUDE_OVERLOAD_MAX_RETRIES; attempt++) {
    try {
      const res = await fetch("https://api.anthropic.com/v1/messages", {
        method: "POST",
        headers,
        body: JSON.stringify(body)
      });

      const rawText = await res.text();
      let data = null;

      if (rawText) {
        try {
          data = JSON.parse(rawText);
        } catch (parseErr) {
          const parseError = new Error(`Claude returned non-JSON response: ${parseErr.message}`);
          parseError.status = res.status;
          parseError.rawText = rawText;
          parseError.isRetryableOverload = isClaudeOverloadError(res.status, rawText);
          throw parseError;
        }
      }

      if (!res.ok) {
        const errMsg = data?.error?.message || `Claude API error (${res.status})`;
        const err = new Error(errMsg);
        err.status = res.status;
        err.data = data;
        err.isRetryableOverload = isClaudeOverloadError(res.status, errMsg);
        throw err;
      }

      const stopReason   = data?.stop_reason || null;
      const outputTokens = data?.usage?.output_tokens || 0;
      const textBlock    = data?.content?.find(block => block.type === "text")?.text;
      const hasThinking  = Boolean(data?.content?.find(b => b.type === "thinking" || b.type === "redacted_thinking"));

      // Always log stop_reason and token counts — critical for diagnosing truncation
      console.log(
        `[callClaude] model=${model} effort=${effort || 'none'} stop_reason=${stopReason} ` +
        `output_tokens=${outputTokens} max_tokens=${maxTokens} ` +
        `has_thinking=${hasThinking} text_len=${textBlock ? textBlock.length : 0}`
      );

      if (!textBlock || !String(textBlock).trim()) {
        const errMsg =
          `Claude returned empty text block ` +
          `(stop_reason=${stopReason}, output_tokens=${outputTokens}, max_tokens=${maxTokens}, ` +
          `has_thinking=${hasThinking}, model=${model}, effort=${effort || 'none'})`;
        console.error(`[callClaude] ${errMsg}`);
        throw new Error(errMsg);
      }

      if (stopReason === "max_tokens") {
        // Log clearly but do not throw — the text block may still be valid JSON
        // if the model finished the JSON before hitting the ceiling. Let safeJsonParse
        // determine whether the content is usable. If it is not, the parse error will
        // surface with the stop_reason already in the console above.
        console.warn(
          `[callClaude] WARNING: stop_reason=max_tokens — response was truncated. ` +
          `output_tokens=${outputTokens}/${maxTokens}. ` +
          `text_len=${textBlock.length}. The JSON parse below will reveal if content is usable.`
        );
      }

      // usage may include cache fields:
      //   cache_creation_input_tokens  — tokens written to cache this call
      //   cache_read_input_tokens      — tokens served from cache this call
      return { text: textBlock, usage: data?.usage || null, stopReason };
    } catch (err) {
      const status = err?.status || null;
      const retryable = Boolean(err?.isRetryableOverload) || isClaudeOverloadError(status, err?.message || "");
      lastError = err;

      if (!retryable || attempt >= CLAUDE_OVERLOAD_MAX_RETRIES) {
        throw err;
      }

      const delayMs = computeClaudeRetryDelayMs(attempt);
      console.warn(
        `[callClaude] retrying Claude request after overload/rate-limit ` +
        `(attempt ${attempt}/${CLAUDE_OVERLOAD_MAX_RETRIES}, model=${model}, status=${status || "n/a"}, delay=${delayMs}ms): ${err.message}`
      );
      await sleep(delayMs);
    }
  }

  throw lastError || new Error("Claude request failed after retries");
}

/* ── helper: strip markdown fences and prose to extract JSON ─── */
/* Used ONLY for the planning phase (Opus), which outputs pure metadata
   strings — no embedded code — so JSON is safe there.               */
function stripFences(text) {
  let cleaned = text
    .replace(/^```json\s*/i, "")
    .replace(/^```\s*/i, "")
    .replace(/\s*```$/i, "")
    .trim();

  const firstBrace = cleaned.indexOf('{');
  const lastBrace = cleaned.lastIndexOf('}');
  if (firstBrace > 0 && lastBrace > firstBrace) {
    cleaned = cleaned.substring(firstBrace, lastBrace + 1);
  }

  return cleaned.trim();
}

function safeJsonParse(text, label) {
  const raw = String(text ?? "");
  if (!raw.trim()) {
    throw new Error(`Failed to parse ${label} output as JSON: empty text (len=${raw.length})`);
  }
  const cleaned = stripFences(raw);
  if (!cleaned.trim()) {
    throw new Error(
      `Failed to parse ${label} output as JSON: no JSON body found after fence stripping ` +
      `(raw len=${raw.length}, preview="${raw.slice(0, 200).replace(/\s+/g, ' ')}")`
    );
  }
  try {
    return JSON.parse(cleaned);
  } catch (error) {
    const preview  = cleaned.slice(0, 300).replace(/\s+/g, ' ');
    const tail     = cleaned.length > 300 ? `...${cleaned.slice(-150).replace(/\s+/g, ' ')}` : '';
    console.error(
      `[safeJsonParse] Failed to parse ${label}: ${error.message} | ` +
      `cleaned_len=${cleaned.length} | preview="${preview}${tail}"`
    );
    throw new Error(
      `Failed to parse ${label} output as JSON: ${error.message} ` +
      `(cleaned_len=${cleaned.length}, preview="${preview.slice(0, 120)}")`
    );
  }
}


function sanitizeChooserChecklist(items = []) {
  return (Array.isArray(items) ? items : []).map((item) => {
    if (typeof item === 'string') {
      return { label: item, delta: 0, evidence: '' };
    }
    return {
      label: String(item?.label || item?.title || item?.reason || '').trim(),
      delta: Number(item?.delta || item?.scoreDelta || 0),
      evidence: String(item?.evidence || item?.summary || item?.reason || '').trim()
    };
  }).filter(item => item.label);
}

function sanitizeChooserScoreboard(items = [], candidateByGroupKey = new Map()) {
  return (Array.isArray(items) ? items : []).map((item) => {
    const groupKey = String(item?.groupKey || item?.candidateId || '').trim();
    const candidate = candidateByGroupKey.get(groupKey) || null;
    return {
      groupKey,
      descriptor: String(item?.descriptor || candidate?.descriptor || '').trim(),
      score: clampChooserScore(item?.score),
      overlayFamilyId: String(item?.overlayFamilyId || item?.familyId || candidate?.overlay?.familyId || '').trim() || null,
      overlayFamilyLabel: String(item?.overlayFamilyLabel || item?.familyLabel || candidate?.overlay?.familyLabel || '').trim() || null,
      hasCombined: item?.hasCombined !== undefined ? !!item.hasCombined : !!candidate?.combined?.present,
      hasOverlay: true,
      topReasons: (Array.isArray(item?.topReasons) ? item.topReasons : Array.isArray(item?.reasons) ? item.reasons : [])
        .map(v => String(v || '').trim())
        .filter(Boolean)
        .slice(0, 8),
      summary: String(item?.summary || '').trim()
    };
  }).filter(item => item.groupKey);
}

function clampChooserScore(value) {
  const n = Number(value);
  if (!Number.isFinite(n)) return 0;
  return Math.max(0, Math.min(100, Math.round(n)));
}

function buildScaffoldChooserSystemPrompt() {
  return `You are the CHERRY3D SCAFFOLD OVERLAY CHOOSER.
Your ONLY job is to compare the Master Game Prompt requirements against EVERY provided Game-Specific Overlay Scaffold candidate and score compatibility.

RULES:
- You are the ONLY authority that decides and scores overlay compatibility.
- Evaluate EVERY candidate. Do not skip any candidate.
- Score each candidate from 0 to 100.
- The winner must be the candidate with the strongest overall compatibility with the Master Game Prompt.
- Strongly penalize explicit conflicts with an overlay's stated prohibitions.
- Strongly reward close matches on camera family, controller/embodiment family, movement authority, world shell family, combat/interaction family, session loop family, subsystem anchors, and UI ownership.
- Prefer candidates whose paired COMBINED scaffold is present.
- Do not choose based on filename resemblance alone.
- Use semantic judgment from the full overlay scaffold content.
- Some overlay_text blocks may end with the marker "[... OVERLAY TEXT TRUNCATED BY CHOOSER PAYLOAD CAP ...]". When you see that marker, score from the head section that is present (signature, anchors, prohibitions, session loop). Do NOT penalize a candidate for truncation — the truncation is a transport cap, not a quality signal.
- Return ONLY valid JSON. No markdown. No commentary outside JSON.

Return JSON with exactly this shape:
{
  "promptRequirements": ["..."],
  "winnerGroupKey": "group_key_here",
  "winnerFamilyId": "family_id_here",
  "winnerFamilyLabel": "family_label_here",
  "winnerFamilyVersion": "version_here",
  "winnerScore": 0,
  "margin": 0,
  "chooserSummary": "one concise paragraph",
  "checklist": [
    { "label": "reason", "delta": 0, "evidence": "brief evidence" }
  ],
  "scoreboard": [
    {
      "groupKey": "group_key_here",
      "descriptor": "descriptor",
      "overlayFamilyId": "family_id",
      "overlayFamilyLabel": "family_label",
      "score": 0,
      "hasCombined": true,
      "topReasons": ["reason 1", "reason 2"],
      "summary": "brief summary"
    }
  ]
}`;
}

function buildScaffoldChooserUserPrompt(promptText = "", candidates = [], metadata = {}) {
  const prompt = String(promptText || "");
  const safeCandidates = Array.isArray(candidates) ? candidates : [];
  const header = [
    `<selector_version>${String(metadata?.selectorVersion || '').trim()}</selector_version>`,
    `<project_name>${String(metadata?.projectName || '').trim()}</project_name>`,
    `<family_hint>${String(metadata?.familyHint?.label || metadata?.familyHint?.id || '').trim()}</family_hint>`,
    `<candidate_count>${safeCandidates.length}</candidate_count>`,
    `<prompt_tokens>${Array.isArray(metadata?.promptTokens) ? metadata.promptTokens.slice(0, 120).join(', ') : ''}</prompt_tokens>`
  ].join("\n");

  const candidateBlocks = safeCandidates.map((candidate) => {
    const overlay = candidate?.overlay || {};
    const combined = candidate?.combined || {};
    const rawOverlayText = String(overlay.text || '').trim();
    const overlayTextTruncated = rawOverlayText.length > SCAFFOLD_CHOOSER_MAX_OVERLAY_CHARS;
    const overlayTextForPrompt = overlayTextTruncated
      ? rawOverlayText.slice(0, SCAFFOLD_CHOOSER_MAX_OVERLAY_CHARS) + SCAFFOLD_CHOOSER_TRUNCATION_MARKER
      : rawOverlayText;
    return `<candidate>
<group_key>${String(candidate?.groupKey || '').trim()}</group_key>
<descriptor>${String(candidate?.descriptor || '').trim()}</descriptor>
<source_folder>${String(candidate?.sourceFolder || '').trim()}</source_folder>
<paired_combined_present>${combined.present ? 'true' : 'false'}</paired_combined_present>
<paired_combined_path>${String(combined.fullPath || combined.archiveFullPath || '').trim()}</paired_combined_path>
<overlay_file_name>${String(overlay.fileName || '').trim()}</overlay_file_name>
<overlay_full_path>${String(overlay.fullPath || overlay.archiveFullPath || '').trim()}</overlay_full_path>
<overlay_family_id>${String(overlay.familyId || '').trim()}</overlay_family_id>
<overlay_family_label>${String(overlay.familyLabel || '').trim()}</overlay_family_label>
<overlay_family_version>${String(overlay.familyVersion || '').trim()}</overlay_family_version>
<overlay_text_char_count>${rawOverlayText.length}</overlay_text_char_count>
<overlay_text_truncated>${overlayTextTruncated ? 'true' : 'false'}</overlay_text_truncated>
<overlay_text>
${overlayTextForPrompt}
</overlay_text>
</candidate>`;
  }).join("\n\n");

  return `${header}

<master_game_prompt>
${prompt}
</master_game_prompt>

<overlay_candidates>
${candidateBlocks}
</overlay_candidates>`;
}

async function chooseScaffoldOverlayWithOpus(apiKey, promptText = "", candidates = [], metadata = {}) {
  const system = buildScaffoldChooserSystemPrompt();
  const userContent = buildScaffoldChooserUserPrompt(promptText, candidates, metadata);
  const result = await callClaude(apiKey, {
    model: SCAFFOLD_CHOOSER_MODEL,
    maxTokens: 24000,
    system,
    userContent,
    effort: 'high',
    useThinking: true
  });

  const parsed = safeJsonParse(result.text, "scaffold chooser");
  const candidateList = Array.isArray(candidates) ? candidates : [];
  const candidateByGroupKey = new Map(candidateList.map(candidate => [String(candidate?.groupKey || '').trim(), candidate]).filter(([key]) => key));

  const winnerGroupKey = String(parsed?.winnerGroupKey || '').trim();
  if (!winnerGroupKey) {
    throw new Error('Scaffold chooser returned no winnerGroupKey.');
  }
  if (!candidateByGroupKey.has(winnerGroupKey)) {
    throw new Error(`Scaffold chooser selected unknown groupKey "${winnerGroupKey}".`);
  }

  const scoreboard = sanitizeChooserScoreboard(parsed?.scoreboard, candidateByGroupKey);
  const winnerCandidate = candidateByGroupKey.get(winnerGroupKey);
  const winnerScore = clampChooserScore(parsed?.winnerScore);
  const sortedScoreboard = scoreboard.length
    ? scoreboard.sort((a, b) => b.score - a.score || a.groupKey.localeCompare(b.groupKey))
    : candidateList.map((candidate) => ({
        groupKey: String(candidate?.groupKey || '').trim(),
        descriptor: String(candidate?.descriptor || '').trim(),
        score: candidate?.groupKey === winnerGroupKey ? winnerScore : 0,
        overlayFamilyId: candidate?.overlay?.familyId || null,
        overlayFamilyLabel: candidate?.overlay?.familyLabel || null,
        hasCombined: !!candidate?.combined?.present,
        hasOverlay: true,
        topReasons: [],
        summary: ''
      })).sort((a, b) => b.score - a.score || a.groupKey.localeCompare(b.groupKey));

  const runnerUp = sortedScoreboard.find(item => item.groupKey !== winnerGroupKey) || null;
  return {
    chooserModel: SCAFFOLD_CHOOSER_MODEL,
    promptRequirements: (Array.isArray(parsed?.promptRequirements) ? parsed.promptRequirements : []).map(v => String(v || '').trim()).filter(Boolean).slice(0, 24),
    winnerGroupKey,
    winnerOverlayPath: winnerCandidate?.overlay?.fullPath || winnerCandidate?.overlay?.archiveFullPath || null,
    winnerFamilyId: String(parsed?.winnerFamilyId || winnerCandidate?.overlay?.familyId || '').trim() || null,
    winnerFamilyLabel: String(parsed?.winnerFamilyLabel || winnerCandidate?.overlay?.familyLabel || '').trim() || null,
    winnerFamilyVersion: String(parsed?.winnerFamilyVersion || winnerCandidate?.overlay?.familyVersion || '').trim() || null,
    winnerScore,
    margin: clampChooserScore(parsed?.margin !== undefined ? parsed.margin : (runnerUp ? winnerScore - runnerUp.score : winnerScore)),
    chooserSummary: String(parsed?.chooserSummary || '').trim(),
    checklist: sanitizeChooserChecklist(parsed?.checklist).slice(0, 12),
    scoreboard: sortedScoreboard
  };
}

/* ── buildArchitectureSpecBlock — REMOVED ─────────────────
   No longer needed. Single-pass planner embeds game-specific
   rules directly in each tranche prompt. ─────────────────── */

const REQUIRED_TRANCHE_VALIDATION_BLOCK = `
VALIDATION + RECOVERY CONTRACT:
- Design tranche prompts so tranche success is judged first by visibleResult + safetyChecks.
- Do NOT plan tranches that depend on stylistic perfection or one preferred coding style to pass.
- Objective scaffold/runtime mistakes may be retried, but only under the runtime policy:
  • 0 retries for soft/advisory findings.
  • 1 retry max for narrow objective hard failures when the repair is obviously surgical.
  • 2 retries only for parser/envelope failures or truly critical scaffold/runtime issues.`;



function buildMasterPromptLayoutGuidance(masterPrompt = "") {
  const prompt = String(masterPrompt || "");
  const hasNewStructuredLayout =
    /#\s*1\.\s*SESSION DECISIONS/i.test(prompt) &&
    /#\s*2\.\s*GAME IDENTITY/i.test(prompt) &&
    /#\s*3\.\s*IMPLEMENTATION CONTRACT/i.test(prompt);
  const hasLegacy63Layout = /\b6\.3(\.\d+)?\b/.test(prompt);

  if (hasNewStructuredLayout) {
    return `MASTER PROMPT LAYOUT DETECTED:
- Section 1 = session decisions / fixed run constraints.
- Section 2 = game identity / fantasy / win-loss / session loop.
- Section 3.x = implementation contract. Treat this as the highest-authority gameplay + technical contract for movement, camera, initialization, overlay, lifecycle, ownership, and exact variables.
- Section 4.x = synopsis matrix. Use 4.1+ for mechanics/rules, world/object inventory, VFX, colours/audio, and authored game content requirements.
- Section 5 = runtime registry / exact names / counts / materials / particle keys / pools.
- Section 6 = author-provided tranche plan. Treat it as advisory sequencing guidance only. Preserve dependency reality, safety, and execution size even if you refine or split it.
- Section 7 = validation contract / hard-fail conditions / non-negotiable outcome checks.
- When this layout is present, NEVER force legacy 6.3 anchors. Use the ACTUAL section numbers from this prompt in anchorSections (for example: 3.1, 3.3, 3.4, 4.1, 4.2, 4.3, 5, 7).
- Sections 3, 4, 5, and 7 are authoritative. Sections 1 and 2 provide context. Section 6 informs sequencing but does not override dependency reality.`;
  }

  if (hasLegacy63Layout) {
    return `MASTER PROMPT LAYOUT DETECTED:
- Legacy 6.3-style structure is present.
- Use the actual 6.3 subsection numbers surfaced by the prompt in anchorSections when they exist.
- Still preserve dependency reality over raw document order.`;
  }

  return `MASTER PROMPT LAYOUT DETECTED:
- No canonical legacy or new layout markers were found.
- Infer the prompt's real section hierarchy from its headings and subheadings.
- Use the ACTUAL headings/subheadings present in the prompt for anchorSections.
- Never invent 6.3 anchors when the prompt does not use them.`;
}

function countOccurrences(haystack, needle) {
  if (!needle) return 0;
  return String(haystack || '').split(needle).length - 1;
}

function parseRosterContractValue(block, pattern) {
  const match = String(block || "").match(pattern);
  return match && match[1] ? String(match[1]).trim() : "";
}

function parseApprovedRosterContracts(approvedRosterBlock = "") {
  const contracts = [];
  const blockRegex = /┌─ \[([^\]]+)\]([\s\S]*?)└─/g;
  let match;

  while ((match = blockRegex.exec(String(approvedRosterBlock || ""))) !== null) {
    const assetName = String(match[1] || "").trim();
    const block = match[0];
    const texturePath = parseRosterContractValue(block, /Colormap path:\s+([^\n]+)/i);
    const scaleWarning = parseRosterContractValue(block, /Scale warning:\s+([^\n]+)/i);
    const dominantAxis = parseRosterContractValue(block, /Dominant axis:\s+([^\n]+)/i);

    contracts.push({
      assetName,
      geometryAvailable: /GEOMETRY CONTRACT \(measured values/i.test(block),
      dominantAxis,
      floorY: parseRosterContractValue(block, /position\.y for floor placement:\s+([^\n]+)/i),
      centerOffsetX: parseRosterContractValue(block, /position\.x centering correction:\s+([^\n]+)/i),
      centerOffsetZ: parseRosterContractValue(block, /position\.z centering correction:\s+([^\n]+)/i),
      suggestedGameScale: parseRosterContractValue(block, /Suggested scale \(largest dim → 1\):\s+([^\n]+)/i),
      scaleVector: parseRosterContractValue(block, /Scale vector:\s+([^\n]+)/i),
      scaleWarning,
      texturePath: /^(NOT AVAILABLE|null|\(not staged\))$/i.test(texturePath) ? "" : texturePath,
      colormapManifestKey: (() => {
        const key = parseRosterContractValue(block, /Colormap key:\s+([^\n]+)/i);
        return /^(NOT AVAILABLE|null|\(unresolved\))$/i.test(key) ? "" : key;
      })(),
      slotCount: Number(parseRosterContractValue(block, /Slot count:\s+([^\n]+)/i) || 0),
      meshCount: Number(parseRosterContractValue(block, /Mesh count:\s+([^\n—]+)/i) || 0)
    });
  }

  return contracts;
}

function promptMentionsAsset(prompt, assetName = "") {
  const promptText = String(prompt || "").toLowerCase();
  const exact = String(assetName || "").trim().toLowerCase();
  const base = exact.replace(/\.[a-z0-9]+$/i, "");
  if (!exact && !base) return false;
  if (exact && promptText.includes(exact)) return true;
  if (base && promptText.includes(base)) return true;
  return false;
}

function buildContractPromptReview(progress, approvedRosterBlock = "") {
  const contracts = parseApprovedRosterContracts(approvedRosterBlock);
  const tranches = Array.isArray(progress?.tranches) ? progress.tranches : [];
  const items = [];
  let reviewedTranches = 0;
  let issueCount = 0;

  tranches.forEach((tranche, index) => {
    const prompt = String(tranche?.prompt || "");
    const referencedAssets = contracts.filter(contract => promptMentionsAsset(prompt, contract.assetName));
    const warnings = [];

    if (referencedAssets.length > 0) {
      reviewedTranches += 1;
    }

    referencedAssets.forEach((contract) => {
    const isAvatarContract = /approved avatars/i.test(approvedRosterBlock) && /ANIMATION CONTRACT/i.test(String(approvedRosterBlock || ''));
      const missing = [];

      if (contract.geometryAvailable) {
        [
          ["floorY", contract.floorY],
          ["centerOffsetX", contract.centerOffsetX],
          ["centerOffsetZ", contract.centerOffsetZ],
          ["suggestedGameScale", contract.suggestedGameScale],
          ["scaleVector", contract.scaleVector],
          ["dominantAxis", contract.dominantAxis]
        ].forEach(([label, value]) => {
          if (value && !prompt.includes(value)) {
            missing.push(`${label}=${value}`);
          }
        });

        if (contract.scaleWarning && contract.scaleWarning.toLowerCase() !== "null" && !prompt.includes(contract.scaleWarning)) {
          missing.push(`scaleWarning=${contract.scaleWarning}`);
        }
      }

      if (contract.texturePath && !prompt.includes(contract.texturePath)) {
        missing.push(`colormapPath=${contract.texturePath}`);
      }
      if (contract.colormapManifestKey && !prompt.includes(contract.colormapManifestKey)) {
        missing.push(`colormapKey=${contract.colormapManifestKey}`);
      }

      if (missing.length > 0) {
        warnings.push(`${contract.assetName}: missing prompt-carried contract values -> ${missing.join(" | ")}`);
      }
    });

    tranche.contractPromptReviewWarnings = warnings;
    tranche.contractPromptReviewStatus = warnings.length > 0 ? "warning" : (referencedAssets.length > 0 ? "ok" : "not_applicable");

    if (warnings.length > 0) {
      issueCount += warnings.length;
    }

    items.push({
      trancheIndex: index,
      trancheName: tranche?.name || `Tranche ${index + 1}`,
      status: tranche.contractPromptReviewStatus,
      assets: referencedAssets.map(contract => contract.assetName),
      warnings
    });
  });

  const summary = issueCount > 0
    ? `Informational tranche prompt contract review: ${issueCount} possible omission(s) across ${reviewedTranches} reviewed tranche(s). Build continues; review the UI log and tranche cards later.`
    : `Informational tranche prompt contract review: no obvious missing carried contract values were detected across ${reviewedTranches} reviewed tranche(s).`;

  return {
    status: "informational",
    generatedAt: Date.now(),
    reviewedTranches,
    issueCount,
    summary,
    items
  };
}


function escapeRegex(value = "") {
  return String(value || "").replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

function buildDeterministicContractAppendixForPrompt(prompt, contracts = []) {
  const referencedAssets = contracts.filter(contract => promptMentionsAsset(prompt, contract.assetName));
  if (referencedAssets.length === 0) return "";
  if (String(prompt || "").includes("=== DETERMINISTIC ROSTER CONTRACT CARRY-THROUGH (AUTO-INJECTED) ===")) {
    return "";
  }

  const lines = referencedAssets.map((contract) => {
    const geometryLines = contract.geometryAvailable ? [
      `  floorY=${contract.floorY || "N/A"}`,
      `  centerOffsetX=${contract.centerOffsetX || "N/A"}`,
      `  centerOffsetZ=${contract.centerOffsetZ || "N/A"}`,
      `  suggestedGameScale=${contract.suggestedGameScale || "N/A"}`,
      `  scaleVector=${contract.scaleVector || "N/A"}`,
      `  dominantAxis=${contract.dominantAxis || "N/A"}`,
      `  scaleWarning=${contract.scaleWarning || "null"}`
    ] : [
      `  geometryContract=NOT AVAILABLE`
    ];

    const slotCount = contract.slotCount || contract.meshCount || 0;
    const textureLines = (contract.texturePath || contract.colormapManifestKey)
      ? [
          `  colormapPath=${contract.texturePath || 'NOT AVAILABLE'}`,
          `  colormapKey=${contract.colormapManifestKey || 'NOT AVAILABLE'}`,
          `  slotCount=${slotCount}`,
          `  meshCount=${contract.meshCount || 0}`,
          `  assetClass=EXTERNAL_NON_PRIMITIVE_SCANNED_OBJECT`,
          `  textureAssignment=Define a registered material whose albedo_texture uses the resolved numeric colormap manifest key ${contract.colormapManifestKey || 'REQUIRED'}, then apply that registered material key across EVERY valid slot N from 0 to ${Math.max(0, (slotCount || 1) - 1)} (${slotCount || 1} slot(s) total) via gameState._applyMat or equivalent slot-safe scaffold logic. material_file must contain the registered material key, never the staged path. Default to createInstance with a registered instance parent, but if working-game law proves per-object visual overrides do not survive instancing for that pool, use createObject consistently instead. Skip this workflow only for the eleven Cherry3D system primitives (cube, square, plane, sphere, cylinder, capsule, cone, torus, torusknot, tetrahedron, icosahedron).`
        ]
      : [
          `  colormapPath=NOT AVAILABLE`,
          `  slotCount=${slotCount}`,
          `  meshCount=${contract.meshCount || 0}`
        ];

    return [
      `- ${contract.assetName}`,
      ...geometryLines,
      ...textureLines
    ].join("\n");
  }).join("\n");

  return `

=== DETERMINISTIC ROSTER CONTRACT CARRY-THROUGH (AUTO-INJECTED) ===
For every asset already named in this tranche, the following roster contract values are mandatory and must be copied verbatim into the emitted code and audit-trail comments. Do not paraphrase, round, or omit them.
${lines}
=== END DETERMINISTIC ROSTER CONTRACT CARRY-THROUGH ===`;
}

function injectDeterministicContractsIntoPlan(plan, approvedRosterBlock = "") {
  const contracts = parseApprovedRosterContracts(approvedRosterBlock);
  const rawTranches = Array.isArray(plan?.tranches) ? plan.tranches : [];
  plan.tranches = rawTranches.map((tranche) => {
    const basePrompt = String(tranche?.prompt || "").trim();
    const appendix = buildDeterministicContractAppendixForPrompt(basePrompt, contracts);
    const prompt = appendix ? `${basePrompt}${appendix}` : basePrompt;
    return {
      ...tranche,
      originalPrompt: tranche?.originalPrompt || basePrompt,
      prompt,
      contractCarryThroughInjected: Boolean(appendix),
      contractCarryThroughAssets: contracts
        .filter(contract => promptMentionsAsset(basePrompt, contract.assetName))
        .map(contract => contract.assetName)
    };
  });
  return plan;
}

function buildContractCodeReviewForTranche(tranche, updatedFiles, approvedRosterBlock = "") {
  const contracts = parseApprovedRosterContracts(approvedRosterBlock);
  const prompt = String(tranche?.prompt || "");
  const combinedCode = Array.isArray(updatedFiles)
    ? updatedFiles.map(file => String(file?.content || "")).join("\n\n")
    : "";
  const referencedAssets = contracts.filter(contract => promptMentionsAsset(prompt, contract.assetName));
  const warnings = [];

  referencedAssets.forEach((contract) => {
    const isAvatarContract = /approved avatars/i.test(approvedRosterBlock) && /ANIMATION CONTRACT/i.test(String(approvedRosterBlock || ''));
    const assetPattern = escapeRegex(contract.assetName);
    const basePattern = escapeRegex(String(contract.assetName || "").replace(/\.[a-z0-9]+$/i, ""));
    const placementAuditPresent = new RegExp(`\\[(?:${assetPattern}|${basePattern})\\]\\s+placement contract applied`, "i").test(combinedCode);
    const textureAuditPresent = new RegExp(`(?:${assetPattern}|${basePattern}).{0,120}applied colormap|applied colormap.{0,120}(?:${assetPattern}|${basePattern})`, "is").test(combinedCode);
    const missing = [];

    if (contract.geometryAvailable) {
      if (!placementAuditPresent) missing.push("placementAuditTrail");
      if (contract.floorY && !combinedCode.includes(contract.floorY)) missing.push(`floorY=${contract.floorY}`);
      if (contract.centerOffsetX && !combinedCode.includes(contract.centerOffsetX)) missing.push(`centerOffsetX=${contract.centerOffsetX}`);
      if (contract.centerOffsetZ && !combinedCode.includes(contract.centerOffsetZ)) missing.push(`centerOffsetZ=${contract.centerOffsetZ}`);
      const hasScaleValue = (contract.scaleVector && combinedCode.includes(contract.scaleVector)) || (contract.suggestedGameScale && combinedCode.includes(contract.suggestedGameScale));
      if (!hasScaleValue) missing.push(`scaleVector|suggestedGameScale=${contract.scaleVector || contract.suggestedGameScale || "N/A"}`);
    }

    if (contract.texturePath || contract.colormapManifestKey) {
      if (!textureAuditPresent) missing.push("textureAuditTrail");
      if (contract.texturePath && !combinedCode.includes(contract.texturePath)) missing.push(`colormapPath=${contract.texturePath}`);
      if (contract.colormapManifestKey && !combinedCode.includes(contract.colormapManifestKey)) missing.push(`colormapKey=${contract.colormapManifestKey}`);

      const hasRegisteredMaterial = /defineMaterial\s*\(/i.test(combinedCode);
      if (!hasRegisteredMaterial) missing.push("defineMaterial(...)");

      const hasSafeApply = /_applyMat\s*\(/.test(combinedCode) || /material_file\s*[:=]/i.test(combinedCode);
      if (!hasSafeApply) missing.push("registeredMaterialApplication");

      const hasCreateInstance = /createInstance\s*\(/.test(combinedCode);
      const hasCreateObjectException = /createObject\s*\(/.test(combinedCode) && /(per-object visual override|instance parent[^\n]{0,80}unsafe|createobject consistently|instancing does not respect)/i.test(combinedCode);
      if (!hasCreateInstance && !hasCreateObjectException) {
        missing.push("createInstance(...)|explicitCreateObjectException");
      }

      const slotCount = contract.slotCount || contract.meshCount || 0;
      if (slotCount > 1 && !/_applyMat\s*\(/.test(combinedCode)) {
        let missedSlots = [];
        for (let slot = 0; slot < slotCount; slot++) {
          const slotPattern = new RegExp(`data\\[['"]${slot}['"]\\]\\.material_file`, "i");
          if (!slotPattern.test(combinedCode)) {
            missedSlots.push(slot);
          }
        }
        if (missedSlots.length > 0) {
          missing.push(`slotCoverageMissing=[${missedSlots.join(",")}] (expected all slots 0-${slotCount - 1} to have registered-material coverage)`);
        }
      }
    }

    const assetLineBlockMatch = String(approvedRosterBlock || '').match(new RegExp(`┌─ \\[${escapeRegex(contract.assetName)}\\]([\\s\\S]*?)└─`, 'i'));
    const assetLineBlock = assetLineBlockMatch ? assetLineBlockMatch[0] : '';
    const clipLine = parseRosterContractValue(assetLineBlock, /Clips:\s+([^\n]+)/i);
    if (clipLine && !/NOT AVAILABLE/i.test(clipLine)) {
      const clips = clipLine.split(',').map(v => v.trim()).filter(Boolean);
      const avatarMentioned = promptMentionsAsset(prompt, contract.assetName);
      if (avatarMentioned) {
        const hasAnyClipEvidence = clips.some(clip => clip && combinedCode.includes(clip));
        if (!hasAnyClipEvidence) missing.push(`animationClip=${clips[0]}`);
      }
    }

    if (missing.length > 0) {
      warnings.push(`${contract.assetName}: emitted code missing contract evidence -> ${missing.join(" | ")}`);
    }
  });

  const approvedAvatarContracts = contracts.filter(contract => {
    const assetLineBlockMatch = String(approvedRosterBlock || '').match(new RegExp(`┌─ \\[${escapeRegex(contract.assetName)}\\]([\\s\\S]*?)└─`, 'i'));
    const assetLineBlock = assetLineBlockMatch ? assetLineBlockMatch[0] : '';
    return /ANIMATION CONTRACT/i.test(assetLineBlock);
  });
  const promptMentionsCharacterRole = /\b(player|enemy|boss|npc|avatar|companion|crowd)\b/i.test(prompt);
  if (promptMentionsCharacterRole && approvedAvatarContracts.length > 0) {
    const codeMentionsApprovedAvatar = approvedAvatarContracts.some(contract => promptMentionsAsset(combinedCode, contract.assetName));
    if (!codeMentionsApprovedAvatar) {
      warnings.push('Tranche handles character role but no approved avatar manifest key was referenced in code.');
    }
  }

  return {
    status: warnings.length > 0 ? "warning" : (referencedAssets.length > 0 ? "ok" : "not_applicable"),
    assets: referencedAssets.map(contract => contract.assetName),
    warnings
  };
}

function summarizeContractCodeReview(progress) {
  const tranches = Array.isArray(progress?.tranches) ? progress.tranches : [];
  let reviewedTranches = 0;
  let issueCount = 0;
  const items = tranches.map((tranche, index) => {
    const warnings = Array.isArray(tranche?.contractCodeReviewWarnings) ? tranche.contractCodeReviewWarnings : [];
    const assets = Array.isArray(tranche?.contractCodeReviewAssets) ? tranche.contractCodeReviewAssets : [];
    if (assets.length > 0) reviewedTranches += 1;
    if (warnings.length > 0) issueCount += warnings.length;
    return {
      trancheIndex: index,
      trancheName: tranche?.name || `Tranche ${index + 1}`,
      status: tranche?.contractCodeReviewStatus || (assets.length > 0 ? "ok" : "not_applicable"),
      assets,
      warnings
    };
  });

  const summary = issueCount > 0
    ? `Informational tranche code contract review: ${issueCount} possible omission(s) across ${reviewedTranches} reviewed tranche(s).`
    : `Informational tranche code contract review: no obvious missing contract evidence was detected across ${reviewedTranches} reviewed tranche(s).`;

  return {
    status: "informational",
    generatedAt: Date.now(),
    reviewedTranches,
    issueCount,
    summary,
    items
  };
}

function selectNextSequentialTranche(progress, preferredIndex = null) {
  const tranches = Array.isArray(progress?.tranches) ? progress.tranches : [];
  const pendingIndices = tranches
    .map((_, index) => index)
    .filter(index => !isTrancheTerminalStatus(tranches[index]?.status));

  if (pendingIndices.length === 0) {
    return { ready: false, done: true, index: null, reason: "all tranches are complete" };
  }

  if (
    Number.isInteger(preferredIndex) &&
    preferredIndex >= 0 &&
    preferredIndex < tranches.length &&
    !isTrancheTerminalStatus(tranches[preferredIndex]?.status)
  ) {
    return { ready: true, done: false, index: preferredIndex };
  }

  return { ready: true, done: false, index: pendingIndices[0] };
}

function normalizeArray(value, fallback = []) {
  if (Array.isArray(value)) return value.filter(Boolean);
  if (value === undefined || value === null || value === '') return [...fallback];
  return [value].filter(Boolean);
}

function enforceTrancheValidationBlock(plan) {
  const rawTranches = Array.isArray(plan?.tranches) ? plan.tranches : [];
  plan.tranches = rawTranches.map((tranche, index) => {
    const expectedFiles = normalizeArray(tranche.expectedFiles || tranche.filesTouched, ['models/2', 'models/23']);
    return {
      kind: tranche.kind || 'build',
      name: tranche.name || `Tranche ${index + 1}`,
      description: tranche.description || tranche.purpose || `Implement tranche ${index + 1}.`,
      anchorSections: normalizeArray(tranche.anchorSections, ['prompt_contract']),
      purpose: tranche.purpose || tranche.description || `Implement tranche ${index + 1}.`,
      systemsTouched: normalizeArray(tranche.systemsTouched, ['gameplay']),
      filesTouched: normalizeArray(tranche.filesTouched, expectedFiles),
      visibleResult: tranche.visibleResult || tranche.description || `Tranche ${index + 1} produces a runnable incremental result.`,
      safetyChecks: normalizeArray(tranche.safetyChecks, tranche.qualityCriteria || ['Leave the project runnable after this tranche.']),
      expertAgents: normalizeArray(tranche.expertAgents, []),
      phase: Number(tranche.phase || 0),
      dependencies: normalizeArray(tranche.dependencies, []),
      qualityCriteria: normalizeArray(tranche.qualityCriteria, []),
      prompt: String(tranche.prompt || '').trim(),
      expectedFiles
    };
  });

  return plan;
}

/* ── helper: parse tranche executor patch-format responses ────── */
/*
   Accepted block delimiters:
   • ===REPLACE_BLOCK: target=== ... ===END_REPLACE_BLOCK: target===
   • ===NEW_FUNCTION: models/2=== ... ===END_NEW_FUNCTION: models/2===
   • ===NEW_FILE: path=== ... ===END_NEW_FILE: path===
   • ===MESSAGE=== ... ===END_MESSAGE===
*/
function parseDelimitedResponse(text) {
  const patches = [];
  let match;

  const newFileRegex = /===NEW_FILE:\s*([^\n]+?)\s*===\n([\s\S]*?)===END_NEW_FILE:\s*\1\s*===/g;
  while ((match = newFileRegex.exec(text)) !== null) {
    patches.push({ path: match[1].trim(), type: "new_file", content: match[2] });
  }

  const replaceRegex = /===REPLACE_BLOCK:\s*([^\n]+?)\s*===\n([\s\S]*?)===END_REPLACE_BLOCK:\s*\1\s*===/g;
  while ((match = replaceRegex.exec(text)) !== null) {
    patches.push({ path: "models/2", type: "replace", target: match[1].trim(), content: match[2] });
  }

  const newFunctionRegex = /===NEW_FUNCTION:\s*models\/2\s*===\n([\s\S]*?)===END_NEW_FUNCTION:\s*models\/2\s*===/g;
  while ((match = newFunctionRegex.exec(text)) !== null) {
    patches.push({ path: "models/2", type: "new_function", content: match[1] });
  }

  const msgMatch = text.match(/===MESSAGE===\n([\s\S]*?)===END_MESSAGE===/);
  const message = msgMatch ? msgMatch[1].trim() : "Tranche completed.";

  if (patches.length === 0) return null;
  return { patches, updatedFiles: [], message, isPatch: true };
}

function escapeRegex(value) {
  return String(value || '').replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

function stripOuterPatchMarkers(content, target) {
  const escapedTarget = escapeRegex(target);
  let inner = String(content || '').replace(/\r\n/g, '\n');
  inner = inner.replace(new RegExp(`^\\s*//\\s*@patch-id:\\s*${escapedTarget}\\s*\\n?`), '');
  inner = inner.replace(new RegExp(`\\n?\\s*//\\s*@end-patch-id:\\s*${escapedTarget}\\s*$`), '');
  return inner.replace(/^\n+/, '').replace(/\n+$/, '');
}

function replaceMarkedBlock(existing, target, rawContent) {
  const startMarker = `// @patch-id: ${target}`;
  const endMarker = `// @end-patch-id: ${target}`;
  const startIdx = existing.indexOf(startMarker);
  if (startIdx < 0) return { matched: false, updated: existing };
  const contentStart = existing.indexOf('\n', startIdx);
  if (contentStart < 0) return { matched: false, updated: existing };
  const endIdx = existing.indexOf(endMarker, contentStart + 1);
  if (endIdx < 0) return { matched: false, updated: existing };

  let innerContent = stripOuterPatchMarkers(rawContent, target);
  if (innerContent) innerContent += '\n';

  return {
    matched: true,
    updated: existing.slice(0, contentStart + 1) + innerContent + existing.slice(endIdx)
  };
}

function parseNewFunctionName(content) {
  const str = String(content || '');
  // Standard declarations: function name(...) or async function name(...)
  let match = str.match(/^\s*(?:async\s+)?function\s+([A-Za-z_$][A-Za-z0-9_$]*)\s*\(/m);
  if (match) return match[1];
  // const/let/var assignments: const name = function / const name = async (...) => / const name = () =>
  match = str.match(/^\s*(?:const|let|var)\s+([A-Za-z_$][A-Za-z0-9_$]*)\s*=\s*(?:async\s+)?(?:function[\s(]|\(|[A-Za-z_$][A-Za-z0-9_$]*\s*=>)/m);
  if (match) return match[1];
  return null;
}

function indentBlock(content, indent) {
  return String(content || '')
    .replace(/\r\n/g, '\n')
    .split('\n')
    .map(line => line ? indent + line : line)
    .join('\n');
}

function applyPatchesToAccumulatedFiles(accumulatedFiles, patches) {
  const warnings = [];
  const touchedPaths = new Set();

  const compilerOwnedWriteGuard = new Set(COMPILER_OWNED_JSON_PATHS);

  for (const patch of patches) {
    const path = patch.path;
    const type = patch.type;
    const content = patch.content;

    if (compilerOwnedWriteGuard.has(path)) {
      warnings.push(`BLOCKED: executor attempted to write compiler-owned path "${path}" — skipped`);
      console.warn(`[PATCH] BLOCKED write to compiler-owned path: ${path}`);
      continue;
    }

    touchedPaths.add(path);

    if (type === "new_file") {
      accumulatedFiles[path] = content;
      console.log(`[PATCH] new_file: ${path} (${content.split("\n").length} lines)`);
      continue;
    }

    const existing = accumulatedFiles[path] || "";

    if (type === "replace") {
      const target = patch.target;
      const replaced = replaceMarkedBlock(existing, target, content);
      if (!replaced.matched) {
        warnings.push(`REPLACE_BLOCK target="${target}" not found in ${path} — skipped`);
        console.warn(`[PATCH] replace skipped: ${path} | target=${target}`);
        continue;
      }
      accumulatedFiles[path] = replaced.updated;
      console.log(`[PATCH] replace: ${path} | target=${target}`);
      continue;
    }

    if (type === "new_function") {
      const functionName = parseNewFunctionName(content);
      if (!functionName) {
        warnings.push(`NEW_FUNCTION in ${path} could not parse a function name — skipped`);
        console.warn(`[PATCH] new_function skipped: ${path} | name parse failed`);
        continue;
      }

      const existingFunctionMarker = `// @patch-id: ${functionName}`;
      if (existing.includes(existingFunctionMarker)) {
        const replaced = replaceMarkedBlock(existing, functionName, content);
        if (!replaced.matched) {
          warnings.push(`NEW_FUNCTION target="${functionName}" existed in ${path} but replacement failed — skipped`);
          console.warn(`[PATCH] new_function reroute skipped: ${path} | target=${functionName}`);
          continue;
        }
        accumulatedFiles[path] = replaced.updated;
        console.log(`[PATCH] new_function rerouted to replace: ${path} | target=${functionName}`);
        continue;
      }

      const helperEndMarker = `// @end-patch-id: zone_helpers`;
      const helperEndIdx = existing.indexOf(helperEndMarker);
      if (helperEndIdx < 0) {
        warnings.push(`NEW_FUNCTION target="${functionName}" could not find zone_helpers in ${path} — skipped`);
        console.warn(`[PATCH] new_function skipped: ${path} | zone_helpers missing`);
        continue;
      }

      const lineStart = existing.lastIndexOf('\n', helperEndIdx);
      const indent = lineStart >= 0 ? (existing.slice(lineStart + 1, helperEndIdx).match(/^\s*/) || [''])[0] : '';
      const rawFunction = String(content || '').trim().replace(/\r\n/g, '\n');
      const markedFunction = [
        `${indent}// @patch-id: ${functionName}`,
        indentBlock(rawFunction, indent),
        `${indent}// @end-patch-id: ${functionName}`,
        ''
      ].join('\n');

      accumulatedFiles[path] = existing.slice(0, helperEndIdx) + markedFunction + existing.slice(helperEndIdx);
      console.log(`[PATCH] new_function: ${path} | target=${functionName}`);
      continue;
    }
  }

  return { touchedPaths: [...touchedPaths], warnings };
}

/* ── helper: save progress to Firebase ───────────────────────── */
async function saveProgress(bucket, projectPath, progress) {
  await bucket.file(`${projectPath}/ai_progress.json`).save(
    JSON.stringify(progress),
    { contentType: "application/json", resumable: false }
  );
}

function buildSceneIntentFreshnessNotice(sceneIntentSyncState) {
  if (!sceneIntentSyncState || !sceneIntentSyncState.staleCompilerOwnedJson) return "";
  const trancheLabel = Number.isInteger(sceneIntentSyncState.lastSceneIntentTrancheIndex)
    ? `Tranche ${sceneIntentSyncState.lastSceneIntentTrancheIndex + 1}`
    : "a prior tranche";
  const trancheName = sceneIntentSyncState.lastSceneIntentTrancheName
    ? ` (${sceneIntentSyncState.lastSceneIntentTrancheName})`
    : "";
  return [
    "=== SCENE INTENT FRESHNESS NOTICE ===",
    `json/scene_intent.json was updated by ${trancheLabel}${trancheName} during this pipeline run.`,
    "json/assets.json, json/tree.json, and json/entities.json are compiler-owned snapshots and may now lag behind the fresher scene_intent.",
    "For all remaining tranches, treat json/scene_intent.json plus models/2 / models/23 as the authoritative live world-state whenever there is any conflict.",
    "Do not emit compiler-owned JSON package files. The frontend compile/apply path will rebuild them after execution finishes.",
    "=== END SCENE INTENT FRESHNESS NOTICE ===",
    ""
  ].join("\n");
}

function buildTrancheFileContextFromAccumulatedFiles(accumulatedFiles, sceneIntentSyncState) {
  // These files are READ-ONLY context for the patch executor.
  // The executor outputs ONLY patch blocks (REPLACE_BLOCK / NEW_FUNCTION / NEW_FILE).
  // The merge engine applies those patches to these files after the tranche completes.
  let trancheFileContext = "CURRENT PROJECT FILES (read-only context — do NOT re-emit these; output only PATCH BLOCKS for your changes):\n\n";
  const compilerOwnedSet = new Set(COMPILER_OWNED_JSON_PATHS);
  const hideCompilerOwnedJson = Boolean(sceneIntentSyncState && sceneIntentSyncState.staleCompilerOwnedJson);

  for (const [path, fileContent] of Object.entries(accumulatedFiles || {})) {
    if (hideCompilerOwnedJson && compilerOwnedSet.has(path)) continue;
    trancheFileContext += `--- FILE: ${path} (READ-ONLY CONTEXT) ---\n${fileContent}\n\n`;
  }

  if (hideCompilerOwnedJson) {
    trancheFileContext += buildSceneIntentFreshnessNotice(sceneIntentSyncState);
  }

  return trancheFileContext;
}

/* ── helper: save ai_response.json with freshness metadata ───── */
/* Called after every successful tranche merge (checkpoint), on
   cancellation, and at final completion so the frontend always has
   the best available snapshot and can verify payload freshness.    */
async function saveAiResponse(bucket, projectPath, allUpdatedFiles, meta = {}) {
  const payload = {
    jobId:         meta.jobId        || "unknown",
    timestamp:     Date.now(),
    trancheIndex:  meta.trancheIndex !== undefined ? meta.trancheIndex : null,
    totalTranches: meta.totalTranches || null,
    status:        meta.status       || "checkpoint", // "checkpoint" | "cancelled" | "final"
    message:       meta.message      || "",
    updatedFiles:  allUpdatedFiles   || [],
    sceneIntentSyncRequired: Boolean(meta.sceneIntentSyncRequired),
    sceneIntentSyncState: meta.sceneIntentSyncState || null
  };
  await bucket.file(`${projectPath}/ai_response.json`).save(
    JSON.stringify(payload),
    { contentType: "application/json", resumable: false }
  );
}

/* ── helper: save pipeline state to Firebase ─────────────────── */
async function savePipelineState(bucket, projectPath, state) {
  await bucket.file(`${projectPath}/ai_pipeline_state.json`).save(
    JSON.stringify(state),
    { contentType: "application/json", resumable: false }
  );
}

/* ── helper: load pipeline state from Firebase ───────────────── */
async function loadPipelineState(bucket, projectPath) {
  const file = bucket.file(`${projectPath}/ai_pipeline_state.json`);
  const [exists] = await file.exists();
  if (!exists) return null;
  const [content] = await file.download();
  return JSON.parse(content.toString());
}

/* ── helper: check kill switch ───────────────────────────────── */
async function checkKillSwitch(bucket, projectPath, jobId) {
  try {
    const activeJobFile = bucket.file(`${projectPath}/ai_active_job.json`);
    const [exists] = await activeJobFile.exists();
    if (exists) {
      const [content] = await activeJobFile.download();
      const activeData = JSON.parse(content.toString());

      if (activeData.jobId && activeData.jobId !== jobId) {
        return { killed: true, reason: "superseded", newJobId: activeData.jobId };
      }
      if (activeData.cancelled) {
        return { killed: true, reason: "cancelled" };
      }
    }
  } catch (e) { /* no active job file = continue safely */ }
  return { killed: false };
}

/* ── helper: self-chain — invoke this function again ─────────── */
async function chainToSelf(payload) {
  const siteUrl = process.env.URL || process.env.DEPLOY_URL || "";
  const chainUrl = `${siteUrl}/.netlify/functions/claudeCodeProxy-background`;

  console.log(`CHAIN → next step: mode=${payload.mode}, tranche=${payload.nextTranche ?? "n/a"} → ${chainUrl}`);

  try {
    const res = await fetch(chainUrl, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload)
    });
    // Background functions return 202 immediately — we don't wait.
    console.log(`Chain response status: ${res.status}`);
  } catch (err) {
    console.error("Chain invocation failed:", err.message);
    throw new Error(`Self-chain failed: ${err.message}`);
  }
}

/* ═══════════════════════════════════════════════════════════════
   SPEC VALIDATION GATE — three Sonnet calls before Opus planning
   ═══════════════════════════════════════════════════════════════

   Call 1 (Extract)  : reads the Master Prompt and identifies the
     game's actual mechanics, producing 6-8 custom simulation
     scenarios tailored to THIS game. Generic enough to work for
     any genre; never hard-codes Fish_Hunt fields.

   Call 2 (Simulate) : traces each scenario through the spec rules
     literally. Documents TRACE / RESULT / ISSUE for each.

   Call 3 (Review)   : classifies simulation findings as PASS/FAIL
     and emits a structured JSON issues list.

   All three calls use claude-sonnet-4-6.
   Thinking is omitted for these calls; use explicit effort only
   so they stay fast and cheap — under ~20 seconds total.
   ═══════════════════════════════════════════════════════════════ */

/* ── Known engine constraints injected into Call 2 ──────────────
   These are scaffold-level facts that the Master Prompt author
   should not have to write — they apply to every Cherry3D game. */
const SCAFFOLD_VALIDATION_CONSTRAINTS = `
KNOWN ENGINE CONSTRAINTS (Cherry3D scaffold v19):
These apply to every game regardless of what the Master Prompt says.
Factor them into your simulations where relevant.

1. OBJECT POOLING: Any spawn/despawn cycle MUST use ScenePool.
   A count cap alone does not prevent WASM object accumulation.
   If the spec has no pooling mechanism, note objectAccumulationRisk.

2. ROOT OBJECT ROTATION: .rotation/.rotate on a root scene object
   is a silent no-op. Directional characters must use scale flip.
   If the spec describes characters that face a travel direction
   with no mechanism stated, note it as a risk.

3. CHILD RIGIDBODY POSITION: A RigidBody added as a child of a
   mesh returns local-space coords from getMotionState(). Top-down
   games must track player position by integrating velocity on the
   main thread — not by reading the child RB.

4. TOP-DOWN CAMERA: mat4.lookAt with a vertical camera produces a
   degenerate matrix. The scaffold handles this automatically —
   no spec action needed, but note it if relevant to the game.

5. COORDINATE SPACE COLLISION: Any angle or position used in a
   collision test must be compared in the same coordinate space.
   If the spec stores child attachment angles in local space but
   compares them directly against world-space angles, note a
   coordinateSpaceCollisionRisk.

6. ENGINE AUTO-ROTATION FROM VELOCITY: Cherry3D can apply visual
   rotation from positional delta. Any object that must keep a fixed
   orientation while moving, flying, or orbiting needs an explicit
   per-frame rotate overwrite. If the spec omits that correction,
   note an autoRotationRisk.

7. INSTANCE PARENT FRUSTUM CULLING: Instanced children inherit
   visibility from the instance parent's bounding box. Hiding the
   instance parent by scaling it to near-zero can make every child
   invisible. Instance parents should be parked off-screen instead.
   If the spec uses near-zero parent scale to hide instance roots,
   note an instancingCullRisk.

8. DOM OVERLAY INPUT OWNERSHIP: The raw overlayRoot provided by the
   engine must remain untouched. Pointer-event toggling belongs on the
   inner game-root element (gameState._gameRootEl), not overlayRoot.
   If a spec implies disabling input on the platform wrapper itself,
   note an overlayInputOwnershipRisk.

9. TEXTURED ROSTER OBJECT CONTRACT: For non-primitive textured scene
   objects, Cherry3D expects the scaffold material-registry path:
   defineMaterial(..., albedo_texture=<resolved numeric colormap key>),
   then apply that registered material key to the object's valid slots.
   material_file must hold the registered material key, not a raw file
   path. The working texture path for these objects is createInstance
   with a registered instance parent. If the spec implies raw-path
   material_file writes for scene objects or relies on plain createObject
   for textured roster geometry, note a texturedObjectContractRisk.

10. CHILD RIGIDBODY LOCAL-SPACE POSITION (Non-Negotiable 13): A RigidBody
   attached as a child of a visual parent operates in local-space. Its
   rbPosition must stay [0,0,0] unless a deliberate local offset is truly
   intended. Passing world-space coordinates into a child rbPosition causes
   POSITION DOUBLING. If the spec passes world coordinates to a child
   rigidbody position, note a childRbPositionDoublingRisk.

11. DYNAMIC VISUAL AUTO-SYNC (Non-Negotiable 16): DYNAMIC visuals do NOT
   always auto-sync with their rigidbody. If the spec moves a DYNAMIC
   actor and expects the visual follows without explicit getMotionState()
   mirroring, note a dynamicVisualDriftRisk. The scaffold provides
   syncDynamicVisualFromRigidBody() for this purpose.

12. TILE-CENTERING AXIS LAW (Non-Negotiable 19): Snap / tile-centering
   correction may ONLY adjust the perpendicular (non-movement) axis.
   For a game moving along Z, only X may be corrected. For a game moving
   along X, only Z may be corrected. If a spec applies centering
   correction to the same axis the player is moving along, note a
   tileSnapAxisViolationRisk.

13. SHARED WASM ASSET CAP (Non-Negotiable 21): The WASM engine enforces one
   hard instance cap per asset globally. Two ScenePools sharing the same
   asset ID and instance parent compete against that single cap. If the spec
   describes two separate pool types using the same geometry for the same
   role (e.g. road tiles and rail tiles both using the same roadStraight
   asset), their combined addObject calls can exceed the cap mid-gameplay
   → OOB crash. Note a sharedAssetCapRisk. They must be declared as a
   single aliased pool with one maxInstances cap.

14. PARTICLE TEMPLATE CROSS-SESSION LEAK (Non-Negotiable 20): Every particle
   template registered in onInit is a live WASM scene object. The engine's
   info worker continues posting position/state updates to their handles
   after session end. If a game registers custom particle templates (any
   ptex_* or game-specific templates beyond particleBillboard/particleSphere)
   but has no explicit teardown mechanism for ALL of them in the session-end
   path, note a particleTemplateleakRisk. Templates must be removed via the
   gameState.particleTemplates registry loop in onDestroy — never via a
   hand-written key list.

15. ASSET READINESS RACE (Non-Negotiable 22): onInit fires before the WASM
   engine has necessarily finished loading every project asset. Calling
   registerInstanceParent or registerParticleTemplate with an asset ID that
   isn't loaded yet dereferences a null pointer → OOB. If the spec registers
   a large number of instance parents or particle templates at startup with
   no readiness check or retry mechanism, note an assetReadinessRaceRisk.
   Burst emitter creation must be deferred past the retry flush via
   _createBurstEmitters() so particlesettings.object is never null.
`;

/* ── Call 1: Extract game-specific simulation scenarios ─────── */
function buildExtractionPrompt(masterPrompt) {
  return `You are a game logic analyst. Read the Master Game Prompt below \
and identify the game's core mechanics that could contain logical errors \
before any code is written.

Produce a JSON array of 6-8 simulation scenarios tailored specifically \
to THIS game. Each scenario must be concrete and traceable — it must \
have a specific setup, a specific spec rule to apply, and a question \
that has a definite numerical or boolean answer.

Cover these four areas for every game:

1. START STATE VIABILITY
   Can the player do anything meaningful in the very first seconds?
   Is there a condition at the exact start value that might be impossible?

2. FIRST INTERACTION CORRECTNESS
   What happens when the player performs the primary action for the
   first time? Does the formula or condition produce the correct result?

3. PROGRESSION FORMULA BEHAVIOUR
   Does the score/growth/currency formula produce smooth progression
   or explosive/broken jumps at representative values?

4. STATE TRANSITION COMPLETENESS
   Do all UI state transitions (death → modal, shop open → close,
   pause → resume, restart) leave the game in a clean defined state?

Also check: does the spec describe a spawn/despawn cycle? If so,
include a scenario that checks whether the spec explicitly requires
object pooling (not just a count cap).

${buildMasterPromptLayoutGuidance(masterPrompt)}

MASTER GAME PROMPT:
${masterPrompt}

Respond with ONLY a valid JSON array. No markdown fences, no preamble.

[
  {
    "id": "SIM-01",
    "area": "start state viability",
    "setup": "exact starting conditions from the spec",
    "specRule": "the relevant rule to quote verbatim from the spec",
    "question": "the specific concrete question to answer",
    "expectedBehaviour": "what correct gameplay looks like here"
  }
]`;
}

/* ── Call 2: Simulate the scenarios against the spec ─────────── */
function buildSimulationPrompt(masterPrompt, scenarios) {
  const scenarioBlock = scenarios.map(s =>
`${s.id} — ${String(s.area || '').toUpperCase()}
  Setup:    ${s.setup}
  Rule:     find and quote verbatim: "${s.specRule}"
  Question: ${s.question}
  Expected: ${s.expectedBehaviour}

  TRACE:  [apply the rule literally, step by step]
  RESULT: [the specific outcome — a number, a state, a behaviour]
  ISSUE:  "none" OR precise description of the problem found`
  ).join('\n\n');

  return `You are a game logic validator. You have been given a Master \
Game Prompt and a set of simulation scenarios tailored to this specific \
game. Trace each scenario through the spec rules literally and document \
exactly what you find.

Do NOT write code. Do NOT summarise the spec. Apply every rule exactly \
as written. If a rule says "strictly less than", apply strictly less than.

${SCAFFOLD_VALIDATION_CONSTRAINTS}

${buildMasterPromptLayoutGuidance(masterPrompt)}

MASTER GAME PROMPT:
${masterPrompt}

SIMULATIONS TO RUN:
${scenarioBlock}

Do not skip any simulation. Do not add simulations not listed above.`;
}

/* ── Call 3: Classify simulation findings as PASS / FAIL ─────── */
function buildReviewPrompt(simulationDoc, scenarios) {
  const simIds = scenarios.map(s => s.id).join(', ');
  return `You are a spec review classifier. Read the simulation document \
below and classify each finding. Do NOT re-run simulations. Do NOT \
introduce new reasoning. ONLY classify what the simulation document \
already found.

Simulation IDs that were run: ${simIds}

SIMULATION DOCUMENT:
${simulationDoc}

Respond with ONLY a valid JSON object. No markdown fences, no preamble.

{
  "result": "PASS" or "FAIL",
  "summary": "one sentence describing the overall finding",
  "issues": [
    {
      "id": "SIM-XX",
      "severity": "CRITICAL" or "HIGH" or "MEDIUM",
      "rule": "the spec rule that is broken, quoted verbatim",
      "description": "precise description of the problem",
      "recommendation": "minimum spec change that fixes this"
    }
  ],
  "passedSimulations": ["SIM-01", "SIM-03"],
  "failedSimulations": ["SIM-02"],
  "objectAccumulationRisk": true or false,
  "startStatePlayable": true or false
}

Classification rules:
- result is FAIL if ANY issue is CRITICAL or HIGH
- result is FAIL if startStatePlayable is false
- result is PASS only if all issues are MEDIUM or lower AND startStatePlayable is true
- startStatePlayable is false if any start-state simulation found the
  player cannot perform the primary action at the initial game state
- objectAccumulationRisk is true if any simulation found a spawn/despawn
  cycle with no pooling requirement stated in the spec
- issues array is empty if all simulations passed cleanly`;
}

/* ── stripArrayFences — strips markdown fences, finds first JSON array ── */
function stripArrayFences(text) {
  let cleaned = text
    .replace(/^```json\s*/i, '')
    .replace(/^```\s*/i, '')
    .replace(/\s*```$/i, '')
    .trim();
  const firstBracket = cleaned.indexOf('[');
  const lastBracket  = cleaned.lastIndexOf(']');
  if (firstBracket >= 0 && lastBracket > firstBracket) {
    cleaned = cleaned.substring(firstBracket, lastBracket + 1);
  }
  return cleaned.trim();
}

/* ── Call 4: Patch — adds missing rules to the Master Prompt ─── */
/* Only called for MEDIUM-severity issues. Never changes an         */
/* existing rule — only adds what is missing.                       */
function buildPatchPrompt(masterPrompt, mediumIssues) {
  const issueBlock = mediumIssues.map((issue, i) =>
`Gap ${i + 1} (${issue.id}):
  Problem:     ${issue.description}
  Missing from: ${issue.rule || 'spec — rule not found or underspecified'}
  Add this:    ${issue.recommendation}`
  ).join('\n\n');

  return `You are a game spec editor. You have been given a Master Game \
Prompt and a list of MEDIUM severity spec gaps — rules that are missing \
or underspecified. Your job is to add the missing rules.

STRICT CONSTRAINTS:
- Add ONLY what is needed to resolve the listed gaps.
- Do NOT change any existing rule, formula, condition, or threshold.
- Do NOT invent new gameplay mechanics.
- Preserve the existing prompt layout and heading hierarchy.
- Insert each missing rule into the most relevant existing section/subsection when that section already exists (for example 3.x, 4.x, 5, or 7). Only append at the end when no relevant section exists.
- Keep the author's section numbering / heading style intact.
- Output the COMPLETE updated Master Prompt — every existing line intact except for the minimal inserted additions.

${buildMasterPromptLayoutGuidance(masterPrompt)}

ORIGINAL MASTER PROMPT:
${masterPrompt}

SPEC GAPS TO RESOLVE:
${issueBlock}

Output the complete updated Master Prompt with the missing rules added.`;
}

/* ── Run one full validation pass (Calls 1-2-3) ─────────────── */
/* Extracted so the retry loop can call it cleanly.               */
async function runSingleValidationPass(apiKey, masterPrompt, scenarios, bucket, projectPath, attempt, imageBlocks = []) {
  const imageValidationPreamble = imageBlocks.length > 0
    ? `\n\nREFERENCE IMAGES: ${imageBlocks.length} game reference image(s) are attached. When evaluating object/entity depth or complexity, treat visual evidence in these images as authoritative. If an image shows more depth, detail, or object complexity than the spec text describes, classify the discrepancy as MEDIUM severity rather than HIGH — the spec may intentionally be terse while the image defines the true target.\n`
    : '';

  // Call 2: Simulate
  const simResult = await callClaude(apiKey, {
    model:       'claude-sonnet-4-6',
    maxTokens:   8000,
    effort:      'low',
    system:      'You are a game logic validator. Be precise and literal.',
    userContent: [
      { type: 'text', text: imageValidationPreamble + buildSimulationPrompt(masterPrompt, scenarios) },
      ...imageBlocks
    ]
  });
  const simulationDoc = simResult.text;

  try {
    await bucket.file(`${projectPath}/ai_validation_simulation${attempt > 0 ? `_patch${attempt}` : ''}.txt`)
      .save(simulationDoc, { contentType: 'text/plain', resumable: false });
  } catch (e) { /* non-fatal */ }

  // Call 3: Review
  const reviewResult = await callClaude(apiKey, {
    model:       'claude-sonnet-4-6',
    maxTokens:   6000,
    effort:      'low',
    system:      'You are a spec review classifier. Respond only with a valid JSON object.',
    userContent: [
      { type: 'text', text: buildReviewPrompt(simulationDoc, scenarios) },
      ...imageBlocks
    ]
  });
  const reviewData = JSON.parse(stripFences(reviewResult.text));

  try {
    await bucket.file(`${projectPath}/ai_validation_review${attempt > 0 ? `_patch${attempt}` : ''}.json`)
      .save(JSON.stringify(reviewData, null, 2), { contentType: 'application/json', resumable: false });
  } catch (e) { /* non-fatal */ }

  return { simulationDoc, reviewData };
}

/* ── Main validation gate orchestrator — with patch retry loop ── */
async function runSpecValidationGate(apiKey, masterPrompt, progress, bucket, projectPath, jobId, imageBlocks = []) {
  console.log(`[VALIDATION] Starting spec validation gate for job ${jobId}`);

  // ── TEMPORARY DISABLE — set to false to re-enable sim 0–8 validation ──
  const VALIDATION_ENABLED = false;
  if (!VALIDATION_ENABLED) {
    console.warn(`[VALIDATION] DISABLED — skipping all sim validations, proceeding directly to planning`);
    progress.status = 'planning';
    progress.validationSkipped = true;
    progress.validationSkipReason = 'Validation temporarily disabled via VALIDATION_ENABLED flag';
    await saveProgress(bucket, projectPath, progress);
    return { passed: true, skipped: true, activePrompt: masterPrompt };
  }

  const MAX_PATCH_ATTEMPTS = 2;

  // Update progress so the frontend shows a validating state
  progress.status = 'validating';
  progress.validationStartTime = Date.now();
  await saveProgress(bucket, projectPath, progress);

  // ── Call 1: Extract scenarios (runs once — same scenarios for all passes) ──
  console.log('[VALIDATION] Call 1: extracting game-specific scenarios...');
  let scenarios;
  try {
    const extractResult = await callClaude(apiKey, {
      model:       'claude-sonnet-4-6',
      maxTokens:   3000,
      effort:      'low',
      system:      'You are a game logic analyst. Respond only with a valid JSON array.',
      userContent: [
        { type: 'text', text: buildExtractionPrompt(masterPrompt) },
        ...imageBlocks
      ]
    });
    scenarios = JSON.parse(stripArrayFences(extractResult.text));
    if (!Array.isArray(scenarios) || scenarios.length === 0) throw new Error('Empty scenario array');
    console.log(`[VALIDATION] Extracted ${scenarios.length} scenario(s): ${scenarios.map(s => s.id).join(', ')}`);
  } catch (e) {
    console.warn(`[VALIDATION] Call 1 failed (${e.message}) — skipping validation, proceeding to planning`);
    progress.status = 'planning';
    progress.validationSkipped = true;
    progress.validationSkipReason = e.message;
    await saveProgress(bucket, projectPath, progress);
    return { passed: true, skipped: true, activePrompt: masterPrompt };
  }

  try {
    await bucket.file(`${projectPath}/ai_validation_scenarios.json`)
      .save(JSON.stringify(scenarios, null, 2), { contentType: 'application/json', resumable: false });
  } catch (e) { /* non-fatal */ }

  progress.validationScenarios = scenarios.map(s => s.id);
  progress.validationCall1Done = true;
  await saveProgress(bucket, projectPath, progress);

  // ── Patch retry loop — Calls 2 + 3 (+ optional Call 4 patch) ────────────
  let activePrompt   = masterPrompt;
  let patchAttempt   = 0;
  let allPatchHistory = [];  // accumulates every patch attempt for the UI

  for (let pass = 0; pass <= MAX_PATCH_ATTEMPTS; pass++) {

    const isRetry = pass > 0;
    console.log(`[VALIDATION] ${isRetry ? `Patch attempt ${pass}/${MAX_PATCH_ATTEMPTS}:` : 'Initial pass:'} running Calls 2+3...`);

    if (isRetry) {
      progress.validationPatchAttempt = pass;
      progress.validationPatchStatus  = 'simulating';
      await saveProgress(bucket, projectPath, progress);
    } else {
      progress.validationCall2Done = false;
      progress.validationCall3Done = false;
      await saveProgress(bucket, projectPath, progress);
    }

    // ── Calls 2 + 3 ────────────────────────────────────────────────────────
    let simulationDoc, reviewData;
    try {
      ({ simulationDoc, reviewData } = await runSingleValidationPass(
        apiKey, activePrompt, scenarios, bucket, projectPath, pass, imageBlocks
      ));
    } catch (e) {
      console.warn(`[VALIDATION] Calls 2/3 failed on pass ${pass} (${e.message}) — skipping, proceeding to planning`);
      progress.status = 'planning';
      progress.validationSkipped = true;
      progress.validationSkipReason = e.message;
      await saveProgress(bucket, projectPath, progress);
      return { passed: true, skipped: true, activePrompt };
    }

    progress.validationCall2Done  = true;
    progress.validationCall3Done  = true;
    progress.validationResult     = reviewData.result;
    progress.validationSummary    = reviewData.summary;
    progress.validationIssues     = reviewData.issues || [];
    progress.validationEndTime    = Date.now();
    await saveProgress(bucket, projectPath, progress);

    console.log(`[VALIDATION] Pass ${pass} result: ${reviewData.result} — ${reviewData.summary}`);

    // ── PASS ────────────────────────────────────────────────────────────────
    if (reviewData.result === 'PASS') {
      progress.status = 'planning';
      progress.validationActivePromptPatched = activePrompt !== masterPrompt;
      await saveProgress(bucket, projectPath, progress);

      // If the prompt was patched, persist the patched version so Opus uses it
      // and preserve the original for the user's reference
      if (activePrompt !== masterPrompt) {
        try {
          await bucket.file(`${projectPath}/ai_validation_original_prompt.txt`)
            .save(masterPrompt, { contentType: 'text/plain', resumable: false });
          await bucket.file(`${projectPath}/ai_validation_patched_prompt.txt`)
            .save(activePrompt, { contentType: 'text/plain', resumable: false });
          console.log(`[VALIDATION] Patched prompt saved. Original preserved.`);
        } catch (e) { /* non-fatal */ }
      }

      return {
        passed:                 true,
        result:                 'PASS',
        summary:                reviewData.summary,
        issues:                 [],
        passedSimulations:      reviewData.passedSimulations || [],
        failedSimulations:      [],
        objectAccumulationRisk: reviewData.objectAccumulationRisk,
        startStatePlayable:     reviewData.startStatePlayable,
        scenarios,
        simulationDoc,
        activePrompt,              // ← caller uses this for Opus, not the original
        wasPatched:             activePrompt !== masterPrompt,
        patchCount:             pass,
        patchHistory:           allPatchHistory
      };
    }

    // ── FAIL — check severity split ─────────────────────────────────────────
    const issues      = reviewData.issues || [];
    const hardIssues  = issues.filter(i => i.severity === 'CRITICAL' || i.severity === 'HIGH');
    const mediumIssues = issues.filter(i => i.severity === 'MEDIUM');

    // Hard failures always halt immediately — never attempt to patch
    if (hardIssues.length > 0) {
      console.log(`[VALIDATION] Hard FAIL (${hardIssues.length} CRITICAL/HIGH) — halting, no auto-patch`);
      progress.status = 'validating';
      await saveProgress(bucket, projectPath, progress);
      return {
        passed:                 false,
        hardStop:               true,
        result:                 'FAIL',
        summary:                reviewData.summary,
        issues,
        passedSimulations:      reviewData.passedSimulations || [],
        failedSimulations:      reviewData.failedSimulations || [],
        objectAccumulationRisk: reviewData.objectAccumulationRisk,
        startStatePlayable:     reviewData.startStatePlayable,
        scenarios,
        simulationDoc,
        activePrompt,
        patchHistory:           allPatchHistory
      };
    }

    // ── MEDIUM-only FAIL — attempt patch if budget remains ──────────────────
    if (pass >= MAX_PATCH_ATTEMPTS) {
      // Budget exhausted
      console.log(`[VALIDATION] Patch budget exhausted after ${pass} attempt(s) — halting`);
      progress.status = 'validating';
      await saveProgress(bucket, projectPath, progress);
      return {
        passed:                 false,
        budgetExhausted:        true,
        result:                 'FAIL',
        summary:                reviewData.summary,
        issues,
        passedSimulations:      reviewData.passedSimulations || [],
        failedSimulations:      reviewData.failedSimulations || [],
        objectAccumulationRisk: reviewData.objectAccumulationRisk,
        startStatePlayable:     reviewData.startStatePlayable,
        scenarios,
        simulationDoc,
        activePrompt,
        patchHistory:           allPatchHistory
      };
    }

    // ── Call 4: Patch ────────────────────────────────────────────────────────
    patchAttempt = pass + 1;
    console.log(`[VALIDATION] MEDIUM-only fail. Running Call 4 (patch attempt ${patchAttempt})...`);
    progress.validationPatchAttempt = patchAttempt;
    progress.validationPatchStatus  = 'patching';
    await saveProgress(bucket, projectPath, progress);

    let patchedPrompt;
    try {
      const patchResult = await callClaude(apiKey, {
        model:       'claude-sonnet-4-6',
        maxTokens:   masterPrompt.length > 20000 ? 16000 : 8000,
        effort:      'low',
        system:      'You are a game spec editor. Output only the updated Master Prompt.',
        userContent: [{ type: 'text', text: buildPatchPrompt(activePrompt, mediumIssues) }]
      });
      patchedPrompt = patchResult.text.trim();
      if (!patchedPrompt || patchedPrompt.length < activePrompt.length * 0.8) {
        throw new Error('Patch produced a truncated or empty prompt');
      }
    } catch (e) {
      console.warn(`[VALIDATION] Call 4 patch failed (${e.message}) — halting validation`);
      progress.status = 'validating';
      await saveProgress(bucket, projectPath, progress);
      return {
        passed:  false,
        result:  'FAIL',
        summary: reviewData.summary,
        issues,
        passedSimulations:      reviewData.passedSimulations || [],
        failedSimulations:      reviewData.failedSimulations || [],
        objectAccumulationRisk: reviewData.objectAccumulationRisk,
        startStatePlayable:     reviewData.startStatePlayable,
        scenarios,
        simulationDoc,
        activePrompt,
        patchHistory: allPatchHistory
      };
    }

    // Save patch artifact
    allPatchHistory.push({ attempt: patchAttempt, issues: mediumIssues.map(i => i.id) });
    try {
      await bucket.file(`${projectPath}/ai_validation_patched_prompt_${patchAttempt}.txt`)
        .save(patchedPrompt, { contentType: 'text/plain', resumable: false });
    } catch (e) { /* non-fatal */ }

    progress.validationPatchStatus  = 'retrying';
    progress.validationPatchHistory = allPatchHistory;
    await saveProgress(bucket, projectPath, progress);

    activePrompt = patchedPrompt;
    console.log(`[VALIDATION] Patch ${patchAttempt} applied (${patchedPrompt.length} chars). Re-running validation...`);
  }

  // Should not reach here — loop exits via return inside
  return { passed: false, result: 'FAIL', activePrompt };
}

/* ═══════════════════════════════════════════════════════════════ */
exports.handler = async (event) => {
  // DIAGNOSTIC: unconditional entry log. If clicking the Update button produces
  // a 500 without this line appearing in Netlify function logs, the request is
  // being rejected at the Netlify edge BEFORE reaching this handler (and the
  // root cause is platform-level: routing, headers, bundle load, etc.)
  // Remove or reduce once the scaffold chooser 500 is diagnosed.
  console.log(
    `[HANDLER_ENTRY] method=${event.httpMethod} path=${event.path} ` +
    `bodyBytes=${event.body ? event.body.length : 0} ` +
    `contentType=${event.headers?.['content-type'] || event.headers?.['Content-Type'] || 'none'} ` +
    `ts=${Date.now()}`
  );

  let projectPath = null;
  let bucket = null;
  let jobId = null;

  try {
    if (!event.body) throw new Error("Missing request body");

    const parsedBody = JSON.parse(event.body);

    const apiKey = process.env.ANTHROPIC_API_KEY;
    if (!apiKey) throw new Error("Missing ANTHROPIC_API_KEY");

    bucket = admin.storage().bucket(process.env.FIREBASE_STORAGE_BUCKET || "gokudatabase.firebasestorage.app");

    // ── Determine mode: "plan" / "tranche" / "scaffold_choose" ───────────
    const mode = parsedBody.mode || "plan";
    const nextTranche = parsedBody.nextTranche || 0;

    if (mode === "scaffold_choose") {
      /* ── Scaffold Overlay Chooser — Firebase staging transport ─────────────────
         Netlify background functions have a hard 256 KB body limit.
         The full chooser payload (~0.24 MB) exceeds this, causing a
         silent 500 with zero function logs. Fix: frontend uploads the
         payload to Firebase first, then POSTs only { mode, projectPath, jobId }.
         This handler reads the staging file and deletes it after loading.
      */
      const chooserProjectPath = parsedBody.projectPath;
      const chooserJobId = parsedBody.jobId;
      if (!chooserProjectPath) throw new Error("Missing projectPath for scaffold chooser");
      if (!chooserJobId) throw new Error("Missing jobId for scaffold chooser");

      projectPath = chooserProjectPath;
      jobId = chooserJobId;

      const chooserArtifactBasePath = `${chooserProjectPath}/ai_scaffold_selection_jobs/${chooserJobId}`;
      const selectionFilePath = `${chooserArtifactBasePath}/ai_scaffold_selection.json`;
      const selectionErrorPath = `${chooserArtifactBasePath}/ai_scaffold_selection_error.json`;
      const stagingFilePath    = `${chooserArtifactBasePath}/ai_scaffold_chooser_request.json`;

      try {
        let stagingPayload;
        try {
          const stagingFile = await bucket.file(stagingFilePath).download();
          stagingPayload = JSON.parse(stagingFile[0].toString("utf8"));
        } catch (stagingErr) {
          throw new Error(
            `Scaffold chooser could not read staging file at ${stagingFilePath}: ${stagingErr.message}. ` +
            `The frontend must upload ai_scaffold_chooser_request.json before dispatching the POST.`
          );
        }

        try { await bucket.file(stagingFilePath).delete(); } catch (_) {}

        const prompt = String(stagingPayload.prompt || "");
        const candidates = Array.isArray(stagingPayload.candidates) ? stagingPayload.candidates : [];
        if (!prompt.trim()) throw new Error("Missing prompt in scaffold chooser staging file");
        if (!candidates.length) throw new Error("Missing candidates in scaffold chooser staging file");

        // Hard candidate-count cap. Do not slice silently — force the
        // caller to curate. Silent slicing is a deterministic fallback
        // (deterministically picking the first N) and that is exactly
        // what the new design rejects.
        if (candidates.length > SCAFFOLD_CHOOSER_MAX_CANDIDATES) {
          throw new Error(
            `Scaffold chooser received ${candidates.length} candidates but the hard cap is ${SCAFFOLD_CHOOSER_MAX_CANDIDATES}. ` +
            `Curate the Family_Scaffold_Sets library or raise SCAFFOLD_CHOOSER_MAX_CANDIDATES deliberately — ` +
            `silently dropping candidates is not allowed.`
          );
        }

        const selection = await chooseScaffoldOverlayWithOpus(apiKey, prompt, candidates, {
          selectorVersion: stagingPayload.selectorVersion || '',
          projectName: stagingPayload.projectName || '',
          familyHint: stagingPayload.familyHint || null,
          promptTokens: stagingPayload.promptTokens || []
        });

        await bucket.file(selectionFilePath).save(
          JSON.stringify({
            success: true,
            jobId: chooserJobId,
            selectorVersion: parsedBody.selectorVersion || '',
            completedTime: Date.now(),
            candidateCount: candidates.length,
            candidateCap: SCAFFOLD_CHOOSER_MAX_CANDIDATES,
            overlayTextCapChars: SCAFFOLD_CHOOSER_MAX_OVERLAY_CHARS,
            selection
          }),
          { contentType: "application/json", resumable: false }
        );

        // Proactively clear a stale error file from a prior failed
        // chooser run so the frontend poller doesn't trip on it.
        try {
          await bucket.file(selectionErrorPath).delete();
        } catch (_) { /* not present — expected on first success */ }

        console.log(`[SCAFFOLD_CHOOSE] jobId=${chooserJobId} winner=${selection?.winnerGroupKey} score=${selection?.winnerScore} candidates=${candidates.length} artifactBase=${chooserArtifactBasePath}`);

        // Return value is discarded by Netlify (background function
        // returns 202 immediately), but still provided for local
        // `netlify dev` visibility and for non-background deploys.
        return {
          statusCode: 200,
          body: JSON.stringify({ success: true, selection })
        };
      } catch (chooserErr) {
        console.error(`[SCAFFOLD_CHOOSE] jobId=${chooserJobId} failed:`, chooserErr);
        try {
          await bucket.file(selectionErrorPath).save(
            JSON.stringify({
              success: false,
              jobId: chooserJobId,
              selectorVersion: parsedBody.selectorVersion || '',
              completedTime: Date.now(),
              error: chooserErr.message || String(chooserErr)
            }),
            { contentType: "application/json", resumable: false }
          );
        } catch (writeErr) {
          console.error(`[SCAFFOLD_CHOOSE] CRITICAL: could not write chooser error file:`, writeErr);
        }

        // Re-throw so the outer catch handles the final response.
        // Returning { statusCode: 500 } synchronously from a background
        // function causes Netlify to discard ALL logs. Re-throwing reaches
        // the outer catch which calls console.error (logs ARE flushed there)
        // and returns 202.
        throw chooserErr;
      }
    }

    projectPath = parsedBody.projectPath;
    jobId = parsedBody.jobId;

    if (!projectPath) throw new Error("Missing projectPath");
    if (!jobId) throw new Error("Missing jobId");

    // ══════════════════════════════════════════════════════════════
    //  MODE: "plan" — First invocation, do planning then chain
    // ══════════════════════════════════════════════════════════════
    if (mode === "plan") {

      // ── 1. Download the request payload from Firebase ─────────
      const requestFile = bucket.file(`${projectPath}/ai_request.json`);
      const [content] = await requestFile.download();
      const requestPayload = JSON.parse(content.toString());
      const { prompt, files, selectedAssets, inlineImages, modelAnalysis, roadPipeline: requestRoadPipeline = null, scaffoldSelection = null } = requestPayload;
      if (!prompt) throw new Error("Missing instructions inside payload");
      const roadPipeline = detectRoadPipelineSettings(prompt, requestRoadPipeline);
      requestPayload.roadPipeline = roadPipeline;
      await requestFile.save(JSON.stringify(requestPayload, null, 2), { contentType: 'application/json', resumable: false });

      // ── 2. Spec Validation Gate ───────────────────────────────
      // Runs three Sonnet calls against the Master Prompt before
      // Opus planning starts. On FAIL, writes ai_error.json with
      // structured issues and halts without invoking Opus.
      // On any internal error the gate is skipped so a bad day
      // at the API doesn't block every game build.
      const earlyProgress = {
        jobId,
        status: 'validating',
        validationStartTime: Date.now()
      };
      await saveProgress(bucket, projectPath, earlyProgress);

      // Build imageBlocks early so validation gate can use them
      const earlyImageBlocks = [];
      if (inlineImages && Array.isArray(inlineImages)) {
        for (const img of inlineImages) {
          if (img.data && img.mimeType && img.mimeType.startsWith('image/')) {
            earlyImageBlocks.push({ type: 'image', source: { type: 'base64', media_type: img.mimeType, data: img.data } });
          }
        }
      }

      const validationResult = await runSpecValidationGate(
        apiKey, prompt, earlyProgress, bucket, projectPath, jobId, earlyImageBlocks
      );

      if (!validationResult.passed && !validationResult.skipped) {
        // Write structured error so the frontend polling loop picks it up
        await bucket.file(`${projectPath}/ai_error.json`).save(
          JSON.stringify({
            error:            `Spec validation FAILED — ${validationResult.issues.length} issue(s) must be resolved in the Master Prompt before code generation can proceed.`,
            jobId,
            validationFailed:    true,
            hardStop:            validationResult.hardStop        || false,
            budgetExhausted:     validationResult.budgetExhausted || false,
            summary:             validationResult.summary,
            issues:              validationResult.issues,
            passedSimulations:   validationResult.passedSimulations,
            failedSimulations:   validationResult.failedSimulations,
            objectAccumulationRisk: validationResult.objectAccumulationRisk,
            startStatePlayable:     validationResult.startStatePlayable,
            patchHistory:        validationResult.patchHistory || []
          }),
          { contentType: 'application/json', resumable: false }
        );
        console.log(`[VALIDATION] FAILED — halting pipeline. Issues: ${validationResult.issues.map(i => i.id).join(', ')}`);
        return { statusCode: 200, body: JSON.stringify({ success: false, validationFailed: true }) };
      }

      console.log(`[VALIDATION] ${validationResult.skipped ? 'SKIPPED (error in gate)' : 'PASSED'} — proceeding to Opus planning`);

      // Use the active prompt (patched or original) for all subsequent pipeline calls
      // If patched, the patched version is already saved in Firebase for the user's reference
      const effectivePrompt = (validationResult.activePrompt) || prompt;

      // ── 3. Build file context string ──────────────────────────
      let fileContext = "Here are the current project files:\n\n";
      if (files) {
        for (const [path, fileContent] of Object.entries(files)) {
          fileContext += `--- FILE: ${path} ---\n${fileContent}\n\n`;
        }
      }

      // ── 3. Build multi-modal content blocks ───────────────────
      const imageBlocks = [];

      if (selectedAssets && Array.isArray(selectedAssets) && selectedAssets.length > 0) {
        let assetContext = "\n\nThe user has designated the following files for use. Their relative paths in the project are:\n";
        for (const asset of selectedAssets) {
          assetContext += `- ${asset.path}\n`;
          const isSupportedImage =
            (asset.type && asset.type.startsWith("image/")) ||
            (asset.name && asset.name.match(/\.(png|jpe?g|webp)$/i));

          if (isSupportedImage) {
            try {
              const assetRes = await fetch(asset.url);
              if (!assetRes.ok) throw new Error(`Failed to fetch: ${assetRes.statusText}`);
              const arrayBuffer = await assetRes.arrayBuffer();
              const base64Data = Buffer.from(arrayBuffer).toString("base64");
              let mime = asset.type;
              if (!mime || !mime.startsWith("image/")) {
                if (asset.name.endsWith(".png")) mime = "image/png";
                else if (asset.name.endsWith(".jpg") || asset.name.endsWith(".jpeg")) mime = "image/jpeg";
                else if (asset.name.endsWith(".webp")) mime = "image/webp";
                else mime = "image/png";
              }
              imageBlocks.push({ type: "image", source: { type: "base64", media_type: mime, data: base64Data } });
            } catch (fetchErr) {
              console.error(`Failed to fetch visual asset ${asset.name}:`, fetchErr);
            }
          } else {
            assetContext += `  (Note: ${asset.name} is a non-image file. Reference it by path in code.)\n`;
          }
        }
        fileContext += assetContext;
      }

      if (modelAnalysis && Array.isArray(modelAnalysis) && modelAnalysis.length > 0) {
        fileContext += `\n\n=== THREE.JS MODEL ANALYSIS ===\n${JSON.stringify(modelAnalysis, null, 2)}\n`;
      }

      if (inlineImages && Array.isArray(inlineImages) && inlineImages.length > 0) {
        for (const img of inlineImages) {
          if (img.data && img.mimeType && img.mimeType.startsWith("image/")) {
            imageBlocks.push({ type: "image", source: { type: "base64", media_type: img.mimeType, data: img.data } });
          }
        }
      }

      // ══════════════════════════════════════════════════════════
      //  SINGLE-PASS PLANNING (Opus 4.6)
      //  Reads Master Prompt + Engine Reference + files directly.
      //  No intermediate architecture spec. No re-synthesis.
      //  Outputs tranche plan with rules embedded in each prompt.
      // ══════════════════════════════════════════════════════════

      // ── Fetch Scaffold + SDK instruction bundle ──
      const instructionBundle = await fetchInstructionBundle(bucket, projectPath, scaffoldSelection?.resolvedInstructionFolder || null);
      assertInstructionBundle(instructionBundle, "PLAN");

      // ── Load approved Asset Roster (if one was approved for this run) ──
      const approvedRosterJson = await loadApprovedRosterJson(bucket, projectPath).catch(() => null);
      const approvedRosterBlock = await loadApprovedRosterBlock(bucket, projectPath);
      if (approvedRosterBlock) {
        console.log("[PLAN] Approved Asset Roster loaded — will be injected into planning and all tranche prompts.");
      }

      const progress = {
        jobId: jobId,
        status: "planning",
        planningStartTime: Date.now(),
        planningEndTime: null,
        planningAnalysis: "",
        totalTranches: 0,
        currentTranche: -1,
        tranches: [],
        tokenUsage: {
          planning: null,
          tranches: [],
          totals: { input_tokens: 0, output_tokens: 0 }
        },
        finalMessage: null,
        error: null,
        completedTime: null,
        contractPromptReview: null,
        scaffoldSelection: scaffoldSelection || null
      };
      await saveProgress(bucket, projectPath, progress);

      const planningSystem = `You are an expert game development planner for the Cherry3D engine.

Your job: read the user's request, the existing project files, and the instruction bundle below. Then split the build into sequential, self-contained TRANCHES that can be executed one at a time by a coding AI.

${instructionBundle.combinedText}${buildPlanningReferencePatternContext(instructionBundle.referencePatternsText)}${buildScaffoldSelectionContext(scaffoldSelection)}

<!-- CACHE_BREAK -->

INSTRUCTION PRECEDENCE:
1. The Cherry3D Scaffold is the binding codified law extracted from shipped working games.
2. The SDK / Engine Notes are subordinate. Use them only for engine facts, API details, threading rules, property paths, and anti-pattern avoidance when the scaffold is silent.
3. If both instruction layers apply to the same topic, the Scaffold wins for architecture, lifecycle shape, immutable sections, required state fields, build sequencing, UI ownership, movement authority, materials/textures, particles, and scene mutation rails.
4. Never elevate the SDK into a parallel lawbook. It fills certainty gaps only where the scaffold does not already settle the issue.
5. Never plan tranches that delete, replace, bypass, or work around an immutable scaffold block. Adapt the requested game to the SELECTED scaffold artifact for this build.
6. Pick one lawful pattern family per subsystem and preserve it through planning, execution, validation, and repair. Do not silently switch families mid-pipeline.
7. REFERENCE IMAGES (if attached): Any images attached to this request are first-class game design inputs with authority equal to the Master Prompt. They define the intended visual style, layout, object types, and complexity level. Where the image and the text spec diverge, treat the image as the authoritative definition of what must be built. Every tranche that involves visual elements, entities, or layouts must reconcile against the attached images.
8. If a tranche needs to declare or modify scene hierarchy, entity placement, visibility, or rigidbody ownership, that tranche must target json/scene_intent.json. scene_intent may use groups[], objects[], and standalone rigidbodies[] when a rigidbody is not attached to a mesh object.
9. Never plan tranches that emit json/assets.json, json/tree.json, or json/entities.json. Those files are compiler-owned and rebuilt in the frontend compile/apply sync path after scene_intent changes. If they conflict with a fresher json/scene_intent.json during this pipeline run, scene_intent.json is authoritative.
10. FILE 23 UI OWNERSHIP RULE: All 2D UI, HUD, menus, bars, timers, flat display panels, and overlay-only interface elements belong in models/23, never in models/2 and never in objects3d.${buildRoadPlanningContext(roadPipeline)}

PLANNING RULES:
1. The Master Prompt's actual contract sections are the center of gravity. Read the real heading structure first, then anchor every core gameplay tranche to the prompt's actual authoritative sections/subsections. If the prompt uses the new layout, prioritize Sections 3.x, 4.x, 5, and 7. If it uses a legacy layout, use those real legacy section numbers. Never invent 6.3 anchors when the prompt does not contain them.
2. Plan the build like a house: foundation before controls, controls before authored playfield shell, shell before gameplay loop, gameplay loop before progression/HUD, progression before feedback/polish.
3. Each tranche prompt must be FULLY SELF-CONTAINED — embed the exact game-specific rules, variable names, slot layouts, code snippets, and pitfall warnings from the user's request that are relevant to that tranche. Do NOT summarize away critical implementation details. Each prompt MUST name the exact zone targets or zone_helpers function names the executor will emit — vague scope like "update the game loop" is a planning defect.
4. ALWAYS split large or complex tranches into A/B/C sub-tranches. There is no hard cap on tranche count — use as many as needed. If in doubt, split.
5. Keep tranche scope TIGHT: each tranche should implement ONE subsystem or ONE cohesive set of closely-related functions. Target 400-500 lines of NEW code per tranche — patch output is flat-cost regardless of accumulated file size, so use the full budget for fidelity and completeness. Only split when there is a true dependency boundary, a distinct risk boundary, or the tranche genuinely covers two independent subsystems that could fail separately. Do NOT split artificially to hit a lower line count.
6. Every tranche must declare: kind, anchorSections, purpose, systemsTouched, filesTouched, visibleResult, safetyChecks, expectedFiles, dependencies, expertAgents, phase, and qualityCriteria.
7. Foundation-A tranche (ALWAYS first): scaffold hook wiring, shared gameState field declarations, objectids registration, and factory/hook extensions that EXTEND the already-selected scaffold artifact. May include materials and constant definitions. Must NOT regenerate scaffold architecture, and must NOT include world geometry placement, terrain, or STATIC rigidbodies — those belong in the immediately following Scene Shell or Terrain Shell tranche. Use the full budget for completeness; a thorough Foundation-A prevents patch collisions in later tranches.
8. Do NOT instruct the executor to remove immutable scaffold fields/blocks or invent a replacement lifecycle when the scaffold already defines one.
9. If the scaffold already provides a section (camera stage, UI hookup, particle emitter factory, instance parent pattern, input handler shape, etc.), the tranche must explicitly extend that section instead of replacing it. If a subsystem requires a lawful pattern choice (for example physics_driven vs direct_integration, createInstance default vs explicit createObject exception, or sphere-burst vs billboard-trail particles), the tranche prompt must name that family explicitly and prohibit mixing.
10. When the user's request contains code examples (updateInput, syncPlayerSharedMemory, ghost AI, etc.), embed those exact code examples in the relevant tranche prompts — do not paraphrase them.
11. Make tranche count DYNAMIC. A simple game with 1-2 mechanics may use 8-10 tranches. Any game with 3+ systems, hazards, audio-feedback, roster assets with texture contracts, or authored level content MUST use 12+ tranches. When in doubt, split — more smaller tranches always beats fewer large ones.
12. Target 400-500 lines of NEW code per tranche. Patch output is flat-cost — the merge engine applies only the changed blocks, so a 500-line patch costs the same as a 50-line patch in terms of output tokens. Use the full budget for game fidelity, complete function bodies, thorough error handling, and rich audio/visual feedback. Only split a tranche when there is a genuine dependency boundary or independent risk boundary — never split purely to reduce line count.
13. LATE-PHASE (tranches 11 and beyond): the 400-500 line budget still applies. Late tranches are for integration, audio balancing, edge cases, and death-sequence polish — these often need substantial code to do well. Split only when a late tranche covers two genuinely independent subsystems that could be validated separately.
15. If an Approved Asset Roster is present, you MUST populate gameState.objectids with every roster asset before the eleven Cherry3D system primitives. Roster assets are mandatory for all non-primitive visual game objects. The eleven Cherry3D system primitives (cube, square, plane, sphere, cylinder, capsule, cone, torus, torusknot, tetrahedron, icosahedron) are reserved for primitive-authored visuals, particle system internals, and invisible collision geometry. If a visual object is intentionally one of those eleven primitives, the tranche prompt must say so explicitly and MUST skip external scan/roster GEOMETRY CONTRACT, TEXTURE CONTRACT, and SLOT CONTRACT enforcement for that object. For every other rendered visual element, the prompt field MUST explicitly name the approved roster asset to use by its resolved objectids manifest key from the Approved Asset Roster block. Using a Cherry3D primitive as a visible gameplay object when a roster asset covers that role is a planning defect. Deprecated model primitive keys 17, 18, 21, 34, and 35 are forbidden everywhere; primitive-authored visuals must resolve only through .primitives keys 4-14.
15a. PRIMITIVE TERRAIN RULE (applies when Road.zip pipeline is NOT active, i.e. roadExclusionFlag is false or absent): All terrain structure — ground floors, terrain floor tiles, mountain body geometry, cliff face geometry, hill shapes, sloped ground planes, raised platforms, and any other structural ground-volume pieces — MUST be built exclusively from Cherry3D system primitives (cube, square, plane, sphere, cylinder, capsule, cone, torus, torusknot, tetrahedron, icosahedron). These are always available in the engine; they require no roster asset. Primitive terrain construction must resolve only through .primitives keys 4-14, never through deprecated model primitive keys 17, 18, 21, 34, or 35. You MUST plan a STANDALONE terrain-shell tranche — NOT merged into Foundation-A or any other tranche — placed at tranche position 1 or 2, immediately after the scaffold foundation tranche and before any tranche that places props, spawns gameplay objects, sets camera distance, or wires gameplay systems. This tranche touches models/2 only and has no dependencies other than the scaffold foundation. Its name MUST contain "Terrain Shell" so it is identifiable in the plan. Every tranche that places props, spawns objects, or positions anything relative to the ground MUST declare the Terrain Shell tranche as an explicit dependency in its dependencies array — a plan that places props in a tranche with no terrain dependency declared is a planning defect. The Terrain Shell tranche MUST include the following in its safetyChecks: "every blocking terrain surface has a STATIC rigidbody (Non-Negotiable 14)", "no terrain geometry sourced from roster or external OBJ asset", and "terrain geometry visually covers the full gameplay area before any prop tranche runs". Its visibleResult field MUST describe the specific ground coverage this game requires (e.g. "flat ground plane covering 200x200 units with STATIC rigidbody" or "three-tier mountain with STATIC rigidbody on each level") — a generic or empty visibleResult is a planning defect. Do NOT plan any tranche that sources terrain floor meshes or mountain body OBJs from the asset roster or from external scanned objects — that is a planning defect. Props that sit ON TOP of the terrain (trees, bushes, rocks, boulders, grass tufts, buildings, ruins, walls, crates, etc.) continue to follow the normal roster-asset rules of rule 15 above.
15b. ROAD-FIRST COMPLIMENTARY PRIMITIVE TERRAIN RULE (applies when Road.zip pipeline IS active, i.e. roadExclusionFlag is true): Road.zip pieces are the authoritative PRIMARY building blocks for road shape, drivable surface, authored track sections, ramps, turns, bumps, and terrain-path layout. However, the sequencer MUST also exploit Cherry3D .primitives keys 4-14 as complimentary terrain-building assets around that assembled Road.zip layout. This primitive work is mandatory for shoulders, roadside verges, runoff, embankments, underfill below elevated pieces, neighboring ground pads, and any remaining terrain continuity not already covered by a Road.zip section. The primitives MUST NOT replace, approximate, or stand in for any road piece that already exists in Road.zip. Every complimentary primitive terrain element must be positioned adjacent to, connected with, and elevation-matched to the placed road sections; visible seams, floating roadside blocks, disconnected filler terrain, or mismatched heights are planning defects. Deprecated model primitive keys 17, 18, 21, 34, and 35 are forbidden everywhere. When Road.zip is active, the injected Road Sequencer tranche MUST explicitly mention both responsibilities: (a) assemble the road from Road.zip first, and (b) stitch the surrounding terrain with adjacent primitives immediately after or alongside accepted road placements so later tranches inherit one connected ground truth.
16. If the Approved Asset Roster contains particle texture entries (particleEffectTarget set), you MUST plan a Foundation-B sub-tranche immediately after Foundation-A. Foundation-B has one job: populate gameState.particleTextureIds keyed by particleEffectTarget using the exact assets.json manifest keys surfaced in the Approved Asset Roster block. Every tranche that registers particle templates MUST declare Foundation-B as a dependency and MUST pass the manifest key as albedo_texture to registerParticleTemplate(). PARTICLE_TEX_PATHS is NOT declared and NOT used — staged Firebase paths never appear in particle template registration. A tranche plan that uses PARTICLE_TEX_PATHS or material_file for particle templates is a planning defect.
17. PARTICLE TEMPLATE APPLICATION RULE: Approved particle textures are applied via the numeric assets.json manifest key as albedo_texture in registerParticleTemplate(). The one authoritative pattern is: registerParticleTemplate({ key, assetId, albedo_texture: gameState.particleTextureIds[effectName] }). Do NOT use extraData: { material_file: PARTICLE_TEX_PATHS[...] } — that pattern is forbidden. Do NOT route particle textures through defineMaterial/_applyMat.
18. GEOMETRY CONTRACT ENFORCEMENT: For every approved roster asset that has a GEOMETRY CONTRACT in the roster block, the tranche whose job is to spawn or position that asset MUST embed the exact numerical values from that contract into its prompt field. Copy floorY, centerOffsetX, centerOffsetZ, scale vector, dominant axis, and any scale warning verbatim. Do NOT paraphrase, estimate, or omit them.
19. TEXTURE CONTRACT ENFORCEMENT: This rule applies to non-primitive approved roster 3D objects and to avatar-path assets whenever the roster surfaces a non-null colormap path / resolved colormap manifest key. For every such asset, the tranche that creates that object MUST plan to define a registered material whose albedo_texture uses the resolved numeric colormap manifest key from assets.json, then apply that registered material key across every valid slot N from 0 to slotCount-1 (fallback meshCount only if slotCount is unavailable) using gameState._applyMat or equivalent slot-safe scaffold logic. material_file must contain the registered material key, never the raw colormap path. Textured scene objects should default to createInstance with a registered instance parent, but if working-game law proves that per-object visual overrides do not survive instancing for that pool, use createObject consistently instead. Cherry3D system primitives skip this external texture-contract workflow. Using defineMaterial() color alone when a colormap is available is a planning defect.
20. SCALE CORRECTION AWARENESS: If an asset's GEOMETRY CONTRACT includes scaleWarning = "LARGE SCALE CORRECTION NEEDED", the tranche prompt MUST explicitly note this and include the suggestedGameScale as the baseline. The executor must apply this baseline before any game-specific size adjustment.
21. SOURCE PRECEDENCE: When the Approved Game-Specific Asset Roster and the raw THREE.JS MODEL ANALYSIS both mention the same asset, treat the roster block as authoritative. Use the raw model analysis only as supporting reference context; never let it override a roster contract value.
22. SLOT CONTRACT: This rule applies ONLY to non-primitive approved roster 3D objects. Every such asset has a slotCount in its TEXTURE CONTRACT (fallback meshCount only if slotCount is unavailable), and the tranche that applies textures MUST cover EVERY valid slot N from 0 to slotCount-1 via gameState._applyMat or equivalent slot-safe scaffold logic. If explicit per-slot assignment is used, material_file must contain the registered material key for every valid slot. Applying only slot 0 when slotCount > 1 is a crash-inducing defect. Applying to a slot index >= slotCount crashes the engine. Cherry3D system primitives skip this slotCount workflow entirely. The slotCount value is a hard constraint, not a suggestion. Embed the exact slotCount value in the tranche prompt so the executor knows the precise loop bounds.
23. HTML UI PLACEMENT RULE: For any tranche that creates or modifies visible HTML UI / HUD / overlay elements in models/23 or localUI, NEVER place UI in the top-left or top-right corners of the screen. Prefer top-center, bottom-center, or clearly inset side placements instead. Any UI that sits near the left or right edge MUST be pulled inward toward the center with visible padding / inset margin rather than hugging the screen edge. Corner-hugging or edge-hugging HUD placement is a planning defect.
24. CHILD RIGIDBODY LOCAL-SPACE POSITION (Non-Negotiable 13): When a RigidBody is attached as a child of a visual object, rbPosition MUST stay [0,0,0] unless a deliberate local offset is truly intended. World-space coordinates passed into a child rbPosition cause POSITION DOUBLING. Every tranche that attaches a RigidBody as a child must carry this constraint explicitly in its prompt.
25. DYNAMIC VISUAL AUTO-SYNC (Non-Negotiable 16): DYNAMIC visuals do NOT auto-sync with their rigidbody in all cases. If a tranche creates DYNAMIC bodies and the visual must track them, the tranche prompt MUST explicitly require either (a) getMotionState() position mirroring every frame in Stage 3, or (b) the scaffold helper syncDynamicVisualFromRigidBody(visualObj, rigidbodyObj). Assuming auto-sync is a planning defect.
26. TILE-CENTERING AXIS LAW (Non-Negotiable 19): Any snap / tile-centering correction may ONLY adjust the non-movement (perpendicular) axis. For a game moving forward along Z, only X may be snapped. For a game moving along X, only Z may be snapped. Any tranche implementing lane-snap, tile-center, or perpendicular correction MUST embed this constraint and use the scaffold helper computePerpendicularCorrection(movementAxis, currentPos, targetPos, gain). Correcting the movement axis itself is a hard defect.
27. ENGINE AUTO-ROTATION FROM VELOCITY (Rule 35): The Cherry3D engine automatically infers and applies a visual rotation to ANY object whose position changes between frames. Any object that must hold a fixed orientation during flight (projectiles, thrown objects, orbiters) must set obj.rotate = [0,0,0] EVERY frame inside its flight-update function — setting it once at spawn is not sufficient. Any tranche spawning or moving such objects MUST embed this requirement explicitly.
28. KINEMATIC DUAL UPDATE (Non-Negotiable 15): KINEMATIC actors moved manually require a dual update — both the visual obj.position AND the collider setPosition must be updated together. The scaffold provides setKinematicDualPose(visualObj, rigidbodyObj, position) for this. Any tranche moving a KINEMATIC actor must explicitly plan the dual update.
29. STATIC COLLISION FOR FLOORS (Non-Negotiable 14): Every floor, track, or ground surface that must block a DYNAMIC actor requires a STATIC rigidbody. A visual-only mesh provides zero collision resistance. Any tranche building ground geometry must explicitly plan a STATIC rigidbody for each blocking surface.

30. PARTICLE TEMPLATE TEARDOWN (Non-Negotiable 20): onDestroy MUST remove every particle template via the gameState.particleTemplates registry loop — never via a hand-written key list. Every template registered through registerParticleTemplate() is automatically tracked and covered by that loop. Any tranche that registers game-specific particle templates (beyond the two scaffold defaults) must declare teardown coverage in its safetyChecks. A tranche that modifies onDestroy must not introduce or preserve a hand-written particle key list — that pattern is FORBIDDEN.

31. SHARED ASSET POOL UNIFICATION (Non-Negotiable 21): If two pool types use the same asset ID and the same instance parent, they MUST be declared as one ScenePool with a single maxInstances cap, aliased to both variable names. Declaring two separate ScenePool instances for one shared WASM asset is FORBIDDEN. canAllocate() MUST be checked before every new addObject call for any capped pool — if canAllocate() returns false, skip allocation entirely rather than calling addObject. In onDestroy (both normal and page-unload paths), every ScenePool MUST call pool.reset() — NOT pool.purge(). purge() parks objects by writing obj.position to handles that are freed after WASM teardown → OOB. reset() is a JS-only handle drop and is always safe in onDestroy.

32. ASSET READINESS AND BURST EMITTER DEFERRAL (Non-Negotiable 22): All burst emitter creation MUST be placed inside a named _createBurstEmitters() function. _createBurstEmitters() is called immediately after the registration retry flush if no particle template retries were queued, or deferred as a queueBuildStep if retries were needed — ensuring particlesettings.object is never null when a burst emitter is created. Any tranche that creates burst emitters must plan them inside _createBurstEmitters() and must not call createParticleEmitter() for burst emitters before the retry flush is confirmed complete. Direct Module.ProjectManager.addObject calls for instance parents or particle templates outside of registerInstanceParent / registerParticleTemplate are FORBIDDEN.

${buildMasterPromptLayoutGuidance(effectivePrompt)}

${REQUIRED_TRANCHE_VALIDATION_BLOCK}

THINKING EFFICIENCY RULE: You have adaptive thinking enabled. Reason efficiently and directly — do not exhaustively re-read sections you have already processed, do not explore dead ends at length, and do not narrate your reasoning steps in detail. Reach your conclusions and begin your JSON output promptly. A thorough, complete JSON output is more valuable than deep internal monologue. Prioritise completeness and correctness of the output JSON over depth of internal reasoning. If you find yourself re-reading the same contract sections repeatedly, stop and commit to your best reading.

You must respond ONLY with a valid JSON object. No markdown, no code fences, no preamble.

{
  "analysis": "Brief planning analysis describing how you decomposed the build and why.",
  "tranches": [
    {
      "kind": "build",
      "name": "Short Name",
      "description": "2-3 sentence description of what this tranche accomplishes.",
      "anchorSections": ["3.1", "4.1"],
      "purpose": "Why this tranche exists in the build order.",
      "systemsTouched": ["player controller", "shared state"],
      "filesTouched": ["models/2", "models/23"],
      "visibleResult": "What the user can observe working after this tranche.",
      "safetyChecks": ["Hard requirements this tranche must satisfy before moving on."],
      "expertAgents": ["agent_id_1", "agent_id_2"],
      "phase": 1,
      "dependencies": [],
      "qualityCriteria": ["Criterion 1", "Criterion 2"],
      "prompt": "THE COMPLETE, SELF-CONTAINED PROMPT for the coding AI. Embed exact game-specific rules, code examples, and pitfall warnings from the user's request. Do NOT repeat the full instruction docs, but ensure the tranche is scaffold-compliant and never violates immutable scaffold sections.",
      "expectedFiles": ["models/2", "models/23"]
    }
  ]
}`;

      const planningUserContent = [
        { type: "text", text: `${approvedRosterBlock}${fileContext}

=== FULL USER REQUEST ===
${effectivePrompt}
=== END USER REQUEST ===` },
        ...imageBlocks
      ];

      // effort: "medium" keeps adaptive thinking active but caps the thinking
      // budget at a fraction of "high", typically finishing in 3-5 minutes
      // rather than 10-15. maxTokens: 64000 is sufficient for any plan up to
      // ~30 tranches with full embedded prompts; 100000 was never consumed
      // and was the primary reason the planner hit the 15-min Lambda limit.
      console.log(`PLANNING: Single-pass Opus 4.7 medium with adaptive thinking for Job ${jobId}...`);
      const planResult = await callClaude(apiKey, {
        model: "claude-opus-4-7",
        maxTokens: 64000,
        effort: "medium",
        useThinking: true,
        system: planningSystem,
        userContent: planningUserContent
      });

      if (planResult.usage) {
        const pu = planResult.usage;
        progress.tokenUsage.planning = pu;
        progress.tokenUsage.totals.input_tokens          += pu.input_tokens             || 0;
        progress.tokenUsage.totals.output_tokens         += pu.output_tokens            || 0;
        progress.tokenUsage.totals.cache_creation_tokens  = (progress.tokenUsage.totals.cache_creation_tokens || 0) + (pu.cache_creation_input_tokens || 0);
        progress.tokenUsage.totals.cache_read_tokens      = (progress.tokenUsage.totals.cache_read_tokens     || 0) + (pu.cache_read_input_tokens     || 0);
        if (pu.cache_read_input_tokens > 0 || pu.cache_creation_input_tokens > 0) {
          console.log(`[CACHE] Planning: created=${pu.cache_creation_input_tokens || 0} read=${pu.cache_read_input_tokens || 0}`);
        }
        // Surface planning diagnostics to progress so the frontend can display them
        progress.planningDiag = {
          stopReason:    planResult.stopReason || null,
          outputTokens:  pu.output_tokens      || 0,
          inputTokens:   pu.input_tokens       || 0,
          maxTokens:     64000,
          effort:        'medium',
          model:         'claude-opus-4-7',
          textLen:       planResult.text ? planResult.text.length : 0,
          capturedAt:    Date.now()
        };
        console.log(`[PLANNING DIAG] stop_reason=${progress.planningDiag.stopReason} output_tokens=${progress.planningDiag.outputTokens}/100000 text_len=${progress.planningDiag.textLen}`);
        await saveProgress(bucket, projectPath, progress);
      }

      let plan = safeJsonParse(planResult.text, "planning");

      if (!plan.tranches || !Array.isArray(plan.tranches) || plan.tranches.length === 0) {
        throw new Error("Planner returned zero tranches.");
      }

      plan = enforceTrancheValidationBlock(plan);
      plan = injectDeterministicContractsIntoPlan(plan, approvedRosterBlock);
      plan = injectFoundationBTranche(plan, approvedRosterJson);
      plan = injectRoadSequencerTranche(plan, roadPipeline);

      // Update progress with plan
      progress.status = "executing";
      progress.planningEndTime = Date.now();
      progress.planningAnalysis = plan.analysis || "";
      progress.totalTranches = plan.tranches.length;
      progress.currentTranche = 0;
      progress.tranches = plan.tranches.map((t, i) => ({
        index: i,
        kind: t.kind || 'build',
        name: t.name,
        description: t.description,
        anchorSections: t.anchorSections || [],
        purpose: t.purpose || t.description || '',
        systemsTouched: t.systemsTouched || [],
        filesTouched: t.filesTouched || t.expectedFiles || [],
        visibleResult: t.visibleResult || '',
        safetyChecks: t.safetyChecks || [],
        expertAgents: t.expertAgents || [],
        phase: t.phase || 0,
        dependencies: t.dependencies || [],
        qualityCriteria: t.qualityCriteria || [],
        prompt: t.prompt === "__ROAD_SEQUENCER_PROMPT__" ? t.prompt : t.prompt,
        expectedFiles: t.expectedFiles || [],
        status: "pending",
        startTime: null,
        endTime: null,
        message: null,
        filesUpdated: [],
        validationRetryCount: 0,
        executionRetryCount: 0,
        retryBudget: 0,
        originalPrompt: t.originalPrompt || t.prompt,
        contractCarryThroughInjected: Boolean(t.contractCarryThroughInjected),
        contractCarryThroughAssets: t.contractCarryThroughAssets || [],
        contractPromptReviewWarnings: [],
        contractPromptReviewStatus: "not_applicable",
        contractCodeReviewWarnings: [],
        contractCodeReviewStatus: "not_applicable",
        contractCodeReviewAssets: []
      }));
      progress.contractPromptReview = buildContractPromptReview(progress, approvedRosterBlock);
      progress.contractCodeReview = summarizeContractCodeReview(progress);
      await saveProgress(bucket, projectPath, progress);

      console.log(`Plan created: ${plan.tranches.length} tranches.`);

      // ── Save pipeline state for chained invocations ──────────
      const pipelineState = {
        jobId,
        projectPath,
        progress,
        accumulatedFiles: files ? { ...files } : {},
        allUpdatedFiles: [],
        imageBlocks,
        modelAnalysis: Array.isArray(modelAnalysis) ? modelAnalysis : [],
        totalTranches: plan.tranches.length,
        approvedRosterBlock,   // ← propagated to every tranche execution
        roadPipeline,
        scaffoldSelection,
        contractPromptReview: progress.contractPromptReview,
        contractCodeReview: progress.contractCodeReview,
        sceneIntentSyncState: {
          staleCompilerOwnedJson: false,
          lastSceneIntentTrancheIndex: null,
          lastSceneIntentTrancheName: null,
          lastSceneIntentTimestamp: null
        }
      };
      await savePipelineState(bucket, projectPath, pipelineState);

      // ── Chain to first tranche ───────────────────────────────
      await chainToSelf({
        projectPath,
        jobId,
        mode: "tranche",
        nextTranche: 0
      });

      return { statusCode: 200, body: JSON.stringify({ success: true, chained: true, phase: "planning_complete" }) };
    }

    // ══════════════════════════════════════════════════════════════
    //  MODE: "tranche" — Execute one tranche, then chain to next
    // ══════════════════════════════════════════════════════════════
    if (mode === "tranche") {

      // ── Kill switch check ────────────────────────────────────
      const killCheck = await checkKillSwitch(bucket, projectPath, jobId);
      if (killCheck.killed) {
        if (killCheck.reason === "superseded") {
          console.log(`Job ${jobId} superseded by ${killCheck.newJobId}. Terminating chain.`);
          return { statusCode: 200, body: JSON.stringify({ success: true, superseded: true }) };
        }
        if (killCheck.reason === "cancelled") {
          console.log("Cancellation signal detected — aborting chain.");
          const state = await loadPipelineState(bucket, projectPath);
          if (state) {
            const activeJobFile = bucket.file(`${projectPath}/ai_active_job.json`);
            await activeJobFile.delete().catch(() => {});
            state.progress.status = "cancelled";
            state.progress.finalMessage = `Pipeline cancelled by user after ${nextTranche} tranche(s).`;
            state.progress.completedTime = Date.now();
            await saveProgress(bucket, projectPath, state.progress);

            if (state.allUpdatedFiles.length > 0) {
              await saveAiResponse(bucket, projectPath, state.allUpdatedFiles, {
                jobId:         state.jobId,
                trancheIndex:  nextTranche,
                totalTranches: state.totalTranches,
                status:        "cancelled",
                message:       `Pipeline cancelled. ${state.allUpdatedFiles.length} file(s) were updated before cancellation.`
              });
            }
          }
          return { statusCode: 200, body: JSON.stringify({ success: true, cancelled: true }) };
        }
      }

      // ── Load pipeline state ──────────────────────────────────
      const state = await loadPipelineState(bucket, projectPath);
      if (!state) throw new Error("Pipeline state not found in Firebase. Chain broken.");

      const { progress, accumulatedFiles, allUpdatedFiles, imageBlocks, modelAnalysis, approvedRosterBlock = "", roadPipeline = null, scaffoldSelection = null, sceneIntentSyncState = null } = state;
      const tranche = progress.tranches[nextTranche];

      // ── Fetch Scaffold + SDK instruction bundle ──
      const instructionBundle = await fetchInstructionBundle(bucket, projectPath, scaffoldSelection?.resolvedInstructionFolder || null);
      assertInstructionBundle(instructionBundle, "TRANCHE");

      if (!tranche) throw new Error(`Tranche ${nextTranche} not found in pipeline state.`);

      // ── Mark tranche as in-progress ──────────────────────────
      progress.currentTranche = nextTranche;
      progress.tranches[nextTranche].status = "in_progress";
      progress.tranches[nextTranche].startTime = progress.tranches[nextTranche].startTime || Date.now();
      await saveProgress(bucket, projectPath, progress);

      console.log(`TRANCHE ${nextTranche + 1}/${progress.totalTranches}: ${tranche.name} (Job ${jobId})`);

      // IMPORTANT: Executors use DELIMITER FORMAT, NOT JSON.
      // Embedding raw JS/HTML code inside JSON string fields causes frequent
      // parse failures because LLMs miss-escape quotes, backslashes, and
      // newlines. Delimiters require zero escaping and are completely robust.
      const executionSystem = `You are an expert game development AI.
The user will provide project files and a focused modification request (one tranche of a larger build).

${instructionBundle.combinedText}${buildScaffoldSelectionContext(scaffoldSelection)}

<!-- CACHE_BREAK -->

INSTRUCTION PRECEDENCE:
- The SELECTED Cherry3D Scaffold artifact is the binding codified law extracted from shipped working games plus the chosen family overlay for this build. Treat it as the required base architecture.
- The SDK / Engine Notes are subordinate. Use them only when the scaffold is silent and engine/API certainty is needed.
- If both apply, the Scaffold wins for architecture, lifecycle, state shape, movement authority, UI ownership, materials/textures, particles, and scene mutation rails.
- Never elevate the SDK into a parallel authority. It fills certainty gaps only where the scaffold does not already settle the issue.
- Never delete, replace, or work around an immutable scaffold section. Extend inside it.
- Pick one lawful pattern family per subsystem and preserve it through the tranche. Do not silently switch families mid-implementation.
- REFERENCE IMAGES (if attached): Any images attached to this tranche are first-class game design inputs with authority equal to the Master Prompt. They define the intended visual appearance, entity types, layout geometry, and interaction model. When implementing this tranche, reconcile your output against the attached images — if your code would produce something visually inconsistent with an attached image, that is a defect. Visual Reconciliation is a required quality criterion for every tranche that touches rendered content.
- SCENE DECLARATION RULE: If scene structure, object placement, hierarchy, visibility, or rigidbody ownership must be declared or changed, write json/scene_intent.json. scene_intent may use groups[], objects[], and standalone rigidbodies[] when a rigidbody must exist directly under a group or at the root.
- COMPILER-OWNED PACKAGE RULE: Never output json/assets.json, json/tree.json, or json/entities.json. They are read-only context files and will be rebuilt by the frontend compile/apply sync path after json/scene_intent.json changes. If those files conflict with a fresher json/scene_intent.json during this pipeline run, json/scene_intent.json is authoritative.
- FILE 23 UI OWNERSHIP RULE: All 2D UI, HUD, menus, bars, timers, flat display panels, and overlay-only interface elements belong in models/23, not models/2 and not objects3d.

Do not re-state the instruction docs — just apply them. Write it correctly the first time so the tranche can move forward without rework.

You must respond using PATCH BLOCK FORMAT only. Do NOT output full file contents. Do NOT use JSON. Do NOT use markdown code blocks.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
OUTPUT FORMAT — THREE BLOCK TYPES ONLY
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
You are given the FULL current file(s) as context above. Do NOT re-emit the full file.
Output ONLY the functions, variables, and sections you are adding or changing in this tranche.
The backend merge engine will apply your patches to the live file.

1. REPLACE_BLOCK — replace a named zone:
===REPLACE_BLOCK: zone_oninit_materials===
// @patch-id: zone_oninit_materials
defineMaterial('mat_red', 255, 0, 0);
// @end-patch-id: zone_oninit_materials
===END_REPLACE_BLOCK: zone_oninit_materials===

   Or replace an existing function in zone_helpers (include the markers):
===REPLACE_BLOCK: spawnVehicle===
// @patch-id: spawnVehicle
function spawnVehicle(config, x, z, direction) {
  // complete updated implementation
}
// @end-patch-id: spawnVehicle
===END_REPLACE_BLOCK: spawnVehicle===

2. NEW_FUNCTION — add a new function to zone_helpers (no markers — engine injects them):
===NEW_FUNCTION: models/2===
function spawnEnemy(type, x, z) {
  // complete implementation
}
===END_NEW_FUNCTION: models/2===

3. NEW_FILE — create a file that does not exist yet:
===NEW_FILE: models/23===
// complete file content
===END_NEW_FILE: models/23===

Always end with:
===MESSAGE===
What this tranche implemented.
===END_MESSAGE===

ZONE TARGETING RULES:
1. Read the full current file before writing anything.
2. For zone_helpers: use NEW_FUNCTION if the function does not exist in the file, REPLACE_BLOCK targeting the function name if it does.
3. For all other models/2 changes: use REPLACE_BLOCK targeting the @patch-id zone name and deliver the complete zone content including everything already there plus your additions.
4. A zone replacement must include ALL existing content in that zone plus the new content — never a partial zone.
5. Never emit INSERT_BLOCK, APPEND_BLOCK, FILE_START, FILE_END, or JSON output.
6. Never emit json/assets.json, json/tree.json, or json/entities.json — compiler-owned, rebuilt automatically.
7. Only include blocks for code you are actually adding or changing in THIS tranche.
8. Do NOT touch code outside this tranche scope — leave it untouched in the existing file.
9. Do NOT replace scaffold-owned state fields with renamed alternatives unless the tranche explicitly requires it.
10. Do NOT invent custom lifecycle blocks when the scaffold already supplies one.

- 3D OBJECT ENFORCEMENT: If an Approved Asset Roster is present and contains objects3d entries, every visible gameplay object introduced or modified in this tranche MUST branch cleanly between the eleven Cherry3D system primitives (cube, square, plane, sphere, cylinder, capsule, cone, torus, torusknot, tetrahedron, icosahedron) and non-primitive approved roster assets. If the object is intentionally one of those eleven primitives, state that explicitly in code comments and skip external scan/roster geometry-texture-meshCount enforcement for that object. For every other visible gameplay object, you MUST use a roster asset via gameState.objectids and the resolved assets.json manifest keys surfaced in the roster block. Using a Cherry3D primitive as a visible gameplay object when a roster asset covers that role is a defect. Those primitives may otherwise only be used for primitive-authored visuals, particle internals, and invisible collision geometry. Deprecated model primitive keys 17, 18, 21, 34, and 35 are forbidden; use only .primitives keys 4-14 when primitive-authored geometry is required.
- PRIMITIVE TERRAIN ENFORCEMENT (applies when Road.zip pipeline is NOT active): All terrain structure built in this tranche — ground floors, terrain floor tiles, mountain body geometry, cliff face geometry, hill shapes, sloped ground planes, raised platforms, and any other structural ground-volume piece — MUST use only the Cherry3D system primitives (cube, square, plane, sphere, cylinder, capsule, cone, torus, torusknot, tetrahedron, icosahedron). Primitive terrain construction must resolve only through .primitives keys 4-14. Never source terrain floor or mountain body geometry from an external OBJ asset or roster entry — that is a defect. Every blocking terrain surface MUST have a STATIC rigidbody (Non-Negotiable 14). If this is the Terrain Shell tranche, you MUST emit a comment block immediately after all terrain geometry is placed that reads: // [TERRAIN SHELL COMPLETE] — lists each primitive used, its .primitives key, and confirms every blocking surface has a STATIC rigidbody. This comment is a required completion proof; its absence is a detectable defect. Props that sit ON TOP of terrain (trees, bushes, rocks, buildings, etc.) continue to use roster assets via gameState.objectids as normal. If this tranche builds terrain shell geometry without using those eleven primitives exclusively, or omits the completion proof comment, that is a defect.
- PARTICLE TEXTURE ENFORCEMENT: Approved particle textures are applied via the numeric assets.json manifest key as albedo_texture in registerParticleTemplate(). The one authoritative pattern is: registerParticleTemplate({ key, assetId, albedo_texture: gameState.particleTextureIds[effectName] }). If this tranche IS Foundation-B, your only job is to populate gameState.particleTextureIds[effectName] with the correct numeric manifest key for every approved particleEffectTarget from the roster block. Do NOT declare PARTICLE_TEX_PATHS. Do NOT use extraData: { material_file: ... } for particle templates. Staged Firebase paths never appear in particle template registration. Using PARTICLE_TEX_PATHS or material_file for particle templates is a defect.
- PLACEMENT MATH AUDIT TRAIL: When placing any roster asset that has a GEOMETRY CONTRACT, include a comment block immediately above the position and scale assignments in the emitted code using the contract values, e.g. // [assetName] placement contract applied: floorY=[v] origin=[class] scale=[s,s,s]. Its absence is a detectable defect.
- TEXTURE ASSIGNMENT AUDIT TRAIL: This applies to non-primitive approved roster 3D objects and to avatar-path assets whenever the roster surfaces a non-null colormap path / resolved colormap manifest key. When creating any such asset, include a comment immediately above the material/setup block noting the applied colormap key and meshCount, define a registered material whose albedo_texture uses that numeric manifest key, and apply that registered material key across every valid slot using gameState._applyMat or equivalent slot-safe scaffold logic. material_file must contain the registered material key, never the raw staged path. Cherry3D system primitives skip this external texture-contract audit trail.
- MESH COUNT CONTRACT (CRASH PREVENTION): This applies ONLY to non-primitive approved roster 3D objects. Every such roster asset carries a meshCount in its TEXTURE CONTRACT. You MUST cover EVERY valid slot N from 0 to meshCount-1 via gameState._applyMat or equivalent slot-safe scaffold logic. If explicit per-slot assignment is used, material_file must carry the registered material key for every valid slot. Assigning only data['0'] when meshCount > 1 leaves untextured mesh slots and CRASHES the engine. Assigning to a slot index >= meshCount also CRASHES the engine. Cherry3D system primitives skip this meshCount workflow entirely. Use a loop or explicit per-slot assignments — never assume a single-slot assignment covers a multi-mesh object. The meshCount value is provided verbatim in the DETERMINISTIC ROSTER CONTRACT CARRY-THROUGH block for this tranche; treat it as a hard loop bound.
- HTML UI PLACEMENT RULE: When this tranche creates or updates visible HTML UI / HUD / overlay elements in models/23 or localUI, NEVER place any UI element in the top-left or top-right corner of the screen. It is always better to move UI slightly inward toward the screen center rather than hugging the left or right edges. Prefer top-center, bottom-center, or clearly inset side placements. Any element near the left or right edge must use an intentional inset margin so it reads as center-biased, not edge-anchored. Top-left / top-right corner placement and hard edge-hugging placement are defects.
- AUDIO FEEL IS NOT OPTIONAL: Whenever this tranche introduces or modifies avatar actions, pickups, rewards, hazards, damage, death, UI state changes, or ambience, you MUST implement the corresponding audio feedback in this tranche unless the tranche prompt explicitly marks the event as stub-only.
- AVATAR FEEDBACK AUDIO: Avatar action sounds must feel soft, appealing, rewarding, and satisfying moment to moment.
- FAILURE / DAMAGE / DEATH AUDIO: Failure, damage, and death sounds must be distinct, higher-detail, clearly impactful, and never harsh or abrasive.
- AMBIENCE RULE: Environmental ambience should be subtle, immersive, and non-intrusive. It must support the setting without masking gameplay-critical cues.
- HAZARD SOUND SIGNATURE RULE: Moving hazards and important moving gameplay objects must have their own identifiable sound signature.
- HAZARD WARNING RULE: Fast or dangerous hazards must receive a short anticipatory warning cue roughly 350-750ms before they enter active play space or before the lethal interaction window opens.
- LETHAL IMPACT FEEDBACK RULE: Major lethal impacts must receive a strong context-appropriate hit effect.
- SEVERE-ONLY DRAMATIC PARTICLES: Violent or dramatic particles are reserved ONLY for severe collision events where the avatar is struck by a major hazard.
- DEATH STAGING RULE: Death sequences must remain visually readable. The avatar must enter a believable defeated pose that matches the force and direction of impact.
- DEATH HOLD RULE: Do NOT reset or hide the avatar immediately on death. Leave enough visible hold time for the player to read the hit reaction, pose, and fail state before restart/reset.
- Treat missing hazard cueing, weak death staging, generic under-signaled lethal hits, abrasive audio, or 'polish later' omissions as tranche execution defects.
- CHILD RIGIDBODY LOCAL-SPACE POSITION (Non-Negotiable 13): When attaching a RigidBody as a child of a visual object, rbPosition MUST stay [0,0,0] unless a deliberate local offset is truly intended. The engine resolves world position through the parent visual transform. Passing world-space coordinates into a child rbPosition causes POSITION DOUBLING — the object appears twice as far from the origin as expected. This is a hard defect with no runtime warning.
- DYNAMIC VISUAL AUTO-SYNC (Non-Negotiable 16): DYNAMIC visuals do NOT always auto-sync with their rigidbody. If this tranche moves a DYNAMIC actor, the visual MUST be explicitly mirrored every frame in Stage 3 of onRender via getMotionState() position readback or the scaffold helper syncDynamicVisualFromRigidBody(visualObj, rigidbodyObj). Assuming the engine will auto-sync the visual is a defect. If the visual ever drifts or freezes while the physics body moves, add the explicit mirror.
- TILE-CENTERING AXIS LAW (Non-Negotiable 19): Any snap / tile-centering / perpendicular correction may ONLY adjust the non-movement axis. If the player moves along Z, only X may be corrected. If the player moves along X, only Z may be corrected. Correcting the movement axis itself stalls the player. Use the scaffold helper computePerpendicularCorrection(movementAxis, currentPos, targetPos, gain) — it enforces this law internally. Never write a manual snap that touches the movement axis.
- ENGINE AUTO-ROTATION FROM VELOCITY (Rule 35): The Cherry3D engine automatically applies visual rotation to ANY object whose position changes between frames. Any object that must hold a fixed orientation during flight (projectiles, thrown objects, balls, knives) MUST set obj.rotate = [0,0,0] EVERY frame inside its flight-update function. Setting rotation once at spawn is not sufficient — the engine overwrites it each frame. For objects orbiting a rotating parent, compute worldAngle = localAngle + parentAngle each frame and set obj.rotate accordingly.
- STATIC COLLISION FOR FLOORS (Non-Negotiable 14): Every floor, track, or ground surface that must block a DYNAMIC actor requires a STATIC rigidbody. Visual-only geometry provides zero collision resistance. Use createStaticGroundBody() or createRigidBody() with motionType='STATIC'.
- KINEMATIC DUAL UPDATE (Non-Negotiable 15): KINEMATIC actors moved manually require a dual update — visual obj.position AND collider setPosition must be updated together every move. Use the scaffold helper setKinematicDualPose(visualObj, rigidbodyObj, position). Updating only one side leaves the collider desynced from the visual.
- PARTICLE TEMPLATE TEARDOWN (Non-Negotiable 20): If this tranche modifies onDestroy, it MUST tear down particle templates via the gameState.particleTemplates registry loop — never via a hand-written key list. The loop covers every key registered through registerParticleTemplate() automatically. Adding or preserving a hard-coded particle key list in onDestroy is a FORBIDDEN pattern.
- SHARED ASSET POOL UNIFICATION (Non-Negotiable 21): Two ScenePools sharing the same asset ID and instance parent MUST be declared as one ScenePool with a single maxInstances cap, aliased to both variable names. In onDestroy (both normal and page-unload paths), call pool.reset() on every ScenePool — NOT pool.purge(). purge() writes obj.position to WASM handles that are freed after teardown → OOB. reset() is JS-only and always safe. canAllocate() MUST be checked before every new addObject call for any capped pool; if false, skip the allocation entirely.
- BURST EMITTER DEFERRAL (Non-Negotiable 22): All burst emitter creation MUST live inside a named _createBurstEmitters() function. Call it immediately after the registration retry flush when no particle template retries were queued, or defer it via queueBuildStep when retries were needed. Never call createParticleEmitter() for burst emitters inline before the retry flush completes — particlesettings.object will be null if the template registration failed. Direct Module.ProjectManager.addObject calls for instance parents or particle templates outside of registerInstanceParent / registerParticleTemplate are FORBIDDEN.

THINKING EFFICIENCY RULE: You have adaptive thinking enabled for this tranche. Reason efficiently and directly — identify the correct scaffold patterns for this tranche's scope, resolve any zone targeting questions, then commit to writing the code. Do not exhaustively re-read instruction sections you have already processed. Do not explore alternative approaches at length when the scaffold already prescribes the correct one. A complete, correct, scaffold-compliant code output is more valuable than deep internal deliberation. Begin writing output as soon as you have resolved the approach.

VALIDATOR STATUS:
- Validation manifest requirements are temporarily disabled.
- Do NOT add VALIDATION_MANIFEST blocks unless another pipeline stage explicitly requires them.
- Focus on correct delimiter output, complete file content, scaffold compliance, and working runtime logic.`;



      // Build file context from accumulated state
      let trancheFileContext = buildTrancheFileContextFromAccumulatedFiles(accumulatedFiles, sceneIntentSyncState);

      if (Array.isArray(modelAnalysis) && modelAnalysis.length > 0) {
        trancheFileContext += `=== THREE.JS MODEL ANALYSIS ===\n${JSON.stringify(modelAnalysis, null, 2)}\n\n`;
      }

      if (progress.contractPromptReview?.issueCount > 0 && Array.isArray(progress.tranches?.[nextTranche]?.contractPromptReviewWarnings) && progress.tranches[nextTranche].contractPromptReviewWarnings.length > 0) {
        console.warn(`[CONTRACT REVIEW][informational] Tranche ${nextTranche + 1}: ${progress.tranches[nextTranche].contractPromptReviewWarnings.join(" || ")}`);
      }

      let tranchePrompt = tranche.prompt;
      if (tranchePrompt === "__ROAD_SEQUENCER_PROMPT__") {
        const roadIndex = await loadRoadIndex(bucket);
        tranchePrompt = buildRoadSequencerPrompt(roadPipeline, roadIndex);
      }

      const rosterPrefix = approvedRosterBlock
        ? `=== APPROVED GAME-SPECIFIC ASSET ROSTER ===\n${approvedRosterBlock}\n=== END ASSET ROSTER ===\n\n`
        : "";
      const referencePatternContext = buildExecutionReferencePatternContext(instructionBundle.referencePatternsText, tranche);

      const trancheUserText = `${buildRoadPlanningContext(roadPipeline)}${referencePatternContext}${rosterPrefix}${trancheFileContext}
=== TRANCHE ${nextTranche + 1} of ${progress.totalTranches}: "${tranche.name}" ===

${tranchePrompt}

=== END TRANCHE INSTRUCTIONS ===

REMINDER: The files above are READ-ONLY context. Output ONLY patch blocks (REPLACE_BLOCK / NEW_FUNCTION / NEW_FILE) for the code you are adding or changing in this tranche. Do NOT re-emit any file in full. The merge engine will apply your patches to the current file content.`;

      const trancheUserContent = [
        {
          type: "text",
          text: trancheUserText
        },
        ...(imageBlocks || [])
      ];

      // ── Model routing: all tranches high effort; Foundation-A gets 100k tokens, others 64k ──
      // Foundation-A wires the most zones simultaneously and is the highest-risk
      // single tranche in the pipeline. All tranches use high effort with adaptive
      // thinking; Foundation-A gets a larger token budget (100k vs 64k).
      const trancheName = String(progress.tranches[nextTranche]?.name || '');
      const isFoundationA = FOUNDATION_A_REGEX.test(trancheName);
      const trancheModel = 'claude-opus-4-7';
      const trancheEffort = 'high';
      const trancheMaxTokens = isFoundationA ? 100000 : 64000;
      const trancheUseThinking = true; // thinking on for all tranches

      console.log(`[TRANCHE ${nextTranche + 1}] ${trancheName} → ${trancheModel} effort=${trancheEffort} max_tokens=${trancheMaxTokens} thinking=${trancheUseThinking}`);

      // ── Stamp AI call start time ─────────────────────────────
      const aiCallStartTime = Date.now();
      progress.tranches[nextTranche].aiCallStartTime = aiCallStartTime;
      await saveProgress(bucket, projectPath, progress);

      let trancheResponseObj;
      try {
        trancheResponseObj = await callClaude(apiKey, {
          model: trancheModel,
          maxTokens: trancheMaxTokens,
          effort: trancheEffort,
          useThinking: trancheUseThinking,
          system: executionSystem,
          userContent: trancheUserContent
        });
      } catch (err) {
        progress.tranches[nextTranche].status = "error";
        progress.tranches[nextTranche].endTime = Date.now();
        progress.tranches[nextTranche].message = `Error: ${err.message}`;
        await saveProgress(bucket, projectPath, progress);
        console.error(`Tranche ${nextTranche + 1} failed:`, err.message);

        // Save state and chain to next tranche (skip this one)
        state.progress = progress;
        await savePipelineState(bucket, projectPath, state);

        // Checkpoint ai_response.json with whatever was accumulated so far
        if (allUpdatedFiles.length > 0) {
          await saveAiResponse(bucket, projectPath, allUpdatedFiles, {
            jobId:         jobId,
            trancheIndex:  nextTranche,
            totalTranches: progress.totalTranches,
            status:        "checkpoint",
            message:       `Checkpoint after tranche ${nextTranche + 1} error-skip. ${allUpdatedFiles.length} file(s) so far.`
          });
        }

        if (nextTranche + 1 < progress.totalTranches) {
          await chainToSelf({ projectPath, jobId, mode: "tranche", nextTranche: nextTranche + 1 });
          return { statusCode: 200, body: JSON.stringify({ success: true, chained: true, phase: `tranche_${nextTranche}_error_skipped` }) };
        }
        // Fall through to finalization if last tranche
      }

      // ── Process tranche response (if we got one) ─────────────
      if (trancheResponseObj) {
        // Stamp AI call end time
        const aiCallEndTime = Date.now();
        progress.tranches[nextTranche].aiCallEndTime = aiCallEndTime;
        progress.tranches[nextTranche].aiCallDurationMs = aiCallEndTime - aiCallStartTime;

        // Record token usage — including prompt-cache fields
        if (trancheResponseObj.usage) {
          const u = trancheResponseObj.usage;
          progress.tokenUsage.tranches[nextTranche] = u;
          progress.tokenUsage.totals.input_tokens            += u.input_tokens             || 0;
          progress.tokenUsage.totals.output_tokens           += u.output_tokens            || 0;
          progress.tokenUsage.totals.cache_creation_tokens   = (progress.tokenUsage.totals.cache_creation_tokens || 0) + (u.cache_creation_input_tokens || 0);
          progress.tokenUsage.totals.cache_read_tokens       = (progress.tokenUsage.totals.cache_read_tokens     || 0) + (u.cache_read_input_tokens     || 0);
          progress.tranches[nextTranche].tokenUsage = u;
          // Store stop_reason, maxTokens, and thinking flag on the tranche for frontend display
          progress.tranches[nextTranche].stopReason    = trancheResponseObj.stopReason || null;
          progress.tranches[nextTranche].maxTokens     = trancheMaxTokens;
          progress.tranches[nextTranche].thinkingUsed  = trancheUseThinking;
          // Log stop_reason clearly — max_tokens here means response was cut short
          console.log(
            `[TRANCHE ${nextTranche + 1} DIAG] stop_reason=${trancheResponseObj.stopReason || 'null'} ` +
            `output_tokens=${u.output_tokens || 0}/${trancheMaxTokens} ` +
            `text_len=${trancheResponseObj.text ? trancheResponseObj.text.length : 0}`
          );
          if (trancheResponseObj.stopReason === 'max_tokens') {
            console.warn(`[TRANCHE ${nextTranche + 1}] WARNING: stop_reason=max_tokens — response was truncated at ${trancheMaxTokens} tokens`);
          }
          // Log cache efficiency per tranche
          if (u.cache_read_input_tokens > 0 || u.cache_creation_input_tokens > 0) {
            console.log(`[CACHE] Tranche ${nextTranche + 1}: created=${u.cache_creation_input_tokens || 0} read=${u.cache_read_input_tokens || 0} (saved ~${Math.round((u.cache_read_input_tokens || 0) * 0.9)} effective tokens)`);
          }
        }

        // Parse using patch format (or legacy full-file fallback)
        const trancheResult = parseDelimitedResponse(trancheResponseObj.text);
        if (!trancheResult) {
          const parseRetryBudget = RETRY_POLICY.parser_envelope;
          const currentParseRetry = progress.tranches[nextTranche].executionRetryCount || 0;

          console.error(`Tranche ${nextTranche + 1} produced no parseable output.`);
          console.error("Raw response (first 500 chars):", trancheResponseObj.text.slice(0, 500));

          if (currentParseRetry < parseRetryBudget) {
            const nextReplay = currentParseRetry + 1;
            progress.tranches[nextTranche].status = "retrying";
            progress.tranches[nextTranche].executionRetryCount = nextReplay;
            progress.tranches[nextTranche].retryBudget = parseRetryBudget;
            progress.tranches[nextTranche].message = `Parser/envelope issue detected — execution replay ${nextReplay}/${parseRetryBudget} queued.`;
            await saveProgress(bucket, projectPath, progress);

            state.progress = progress;
            await savePipelineState(bucket, projectPath, state);

            await chainToSelf({ projectPath, jobId, mode: "tranche", nextTranche });
            return { statusCode: 200, body: JSON.stringify({ success: true, chained: true, phase: `tranche_${nextTranche}_parse_retry_${nextReplay}` }) };
          }

          progress.tranches[nextTranche].status = "error";
          progress.tranches[nextTranche].endTime = Date.now();
          progress.tranches[nextTranche].message = `Executor returned no recognisable patch blocks after ${parseRetryBudget} parser/envelope retries.`;
          await saveProgress(bucket, projectPath, progress);
          console.error(`Tranche ${nextTranche + 1} produced no parseable output.`);
          console.error("Raw response (first 500 chars):", trancheResponseObj.text.slice(0, 500));

          state.progress = progress;
          await savePipelineState(bucket, projectPath, state);

          if (allUpdatedFiles.length > 0) {
            await saveAiResponse(bucket, projectPath, allUpdatedFiles, {
              jobId:         jobId,
              trancheIndex:  nextTranche,
              totalTranches: progress.totalTranches,
              status:        "checkpoint",
              message:       `Checkpoint after tranche ${nextTranche + 1} parser/envelope exhaustion. ${allUpdatedFiles.length} file(s) so far.`,
              sceneIntentSyncRequired: Boolean(state.sceneIntentSyncState?.staleCompilerOwnedJson),
              sceneIntentSyncState: state.sceneIntentSyncState || null
            });
          }

          if (nextTranche + 1 < progress.totalTranches) {
            await chainToSelf({ projectPath, jobId, mode: "tranche", nextTranche: nextTranche + 1 });
            return { statusCode: 200, body: JSON.stringify({ success: true, chained: true, phase: `tranche_${nextTranche}_parse_error` }) };
          }
          // Fall through to finalization
        }

        if (trancheResult) {
          const trancheFilesUpdated = [];

          // ── PATCH PATH: apply named blocks into accumulated files ──
          if (trancheResult.isPatch && Array.isArray(trancheResult.patches) && trancheResult.patches.length > 0) {
            const mergeStartTime = Date.now();
            const { touchedPaths, warnings: mergeWarnings } = applyPatchesToAccumulatedFiles(
              accumulatedFiles,
              trancheResult.patches
            );
            const mergeEndTime = Date.now();
            progress.tranches[nextTranche].mergeStartTime = mergeStartTime;
            progress.tranches[nextTranche].mergeEndTime   = mergeEndTime;
            progress.tranches[nextTranche].mergeDurationMs = mergeEndTime - mergeStartTime;

            // ── Integrity check: log only — build always continues ──────────
            // Duplicate declarations and orphaned PATCH WARNING blocks are
            // recorded on the tranche for UI visibility. The executor collision
            // guard (Fix 3a) and patch-format repair prompts (Fix 3b) are the
            // primary defences. The final validator catches anything that slips
            // through. Never halt here — a warning is not a build failure.
            const integrityIssues = mergeWarnings.filter(w =>
              w.startsWith("DUPLICATE DECLARATION") || w.startsWith("ORPHANED BLOCK")
            );
            const routineWarnings = mergeWarnings.filter(w =>
              !w.startsWith("DUPLICATE DECLARATION") && !w.startsWith("ORPHANED BLOCK")
            );
            if (routineWarnings.length > 0) {
              progress.tranches[nextTranche].patchMergeWarnings = routineWarnings;
              console.warn(`[PATCH MERGE] Tranche ${nextTranche + 1} warnings: ${routineWarnings.join(" | ")}`);
            }
            if (integrityIssues.length > 0) {
              progress.tranches[nextTranche].patchIntegrityIssues = integrityIssues;
              progress.tranches[nextTranche].patchMergeWarnings = [
                ...(progress.tranches[nextTranche].patchMergeWarnings || []),
                ...integrityIssues
              ];
              console.warn(`[MERGE INTEGRITY] Tranche ${nextTranche + 1} — issues logged, build continues: ${integrityIssues.join(" | ")}`);
            }

            // Record patch block stats for UI display
            progress.tranches[nextTranche].patchBlockCount = trancheResult.patches.length;
            progress.tranches[nextTranche].patchBlockTypes = trancheResult.patches.map(p => p.type);
            progress.tranches[nextTranche].patchLinesAdded = trancheResult.patches.reduce(
              (sum, p) => sum + (p.content ? p.content.split("\n").length : 0), 0
            );

            for (const path of touchedPaths) {
              trancheFilesUpdated.push(path);
              // Sync allUpdatedFiles with the now-merged accumulated content
              const mergedContent = accumulatedFiles[path];
              const existingIdx = allUpdatedFiles.findIndex(f => f.path === path);
              if (existingIdx >= 0) {
                allUpdatedFiles[existingIdx] = { path, content: mergedContent };
              } else {
                allUpdatedFiles.push({ path, content: mergedContent });
              }
            }

            // ── Fix 3: zero-write detection ────────────────────────────────
            // Patches were parsed but every one was skipped (bad zone names,
            // compiler-owned blocks, or missing markers). Nothing was written.
            // Flag it explicitly so it is visible in the UI and logs — the
            // build continues, but a silent no-op is never treated as success.
            if (touchedPaths.length === 0) {
              const zeroWriteMsg = `Tranche ${nextTranche + 1}: ${trancheResult.patches.length} patch block(s) parsed but zero files written — all patches skipped (check merge warnings above).`;
              progress.tranches[nextTranche].patchMergeWarnings = [
                ...(progress.tranches[nextTranche].patchMergeWarnings || []),
                `ZERO WRITE: ${zeroWriteMsg}`
              ];
              console.warn(`[PATCH MERGE] ${zeroWriteMsg}`);
            }

            console.log(`[PATCH] Tranche ${nextTranche + 1} merged ${trancheResult.patches.length} patch block(s) into ${touchedPaths.length} file(s).`);

          } else {
            // Fix 4: the legacy full-file-output path is unreachable under the
            // zone-based architecture — parseDelimitedResponse always returns
            // isPatch:true with updatedFiles:[]. If this branch ever fires,
            // something upstream changed and must be investigated immediately.
            console.error(`[PATCH] Tranche ${nextTranche + 1}: unexpected code path — trancheResult has no patch blocks and no updatedFiles. Response shape: isPatch=${trancheResult.isPatch}, patches=${JSON.stringify(trancheResult.patches)?.slice(0, 200)}, updatedFiles=${JSON.stringify(trancheResult.updatedFiles)?.slice(0, 200)}`);
          }

          // ── scene_intent detection (works for both paths) ────────────
          const sceneIntentTouchedThisTranche = trancheFilesUpdated.some(
            p => String(p).trim() === "json/scene_intent.json"
          );
          if (sceneIntentTouchedThisTranche) {
            state.sceneIntentSyncState = {
              staleCompilerOwnedJson: true,
              lastSceneIntentTrancheIndex: nextTranche,
              lastSceneIntentTrancheName: tranche.name || null,
              lastSceneIntentTimestamp: Date.now()
            };
            progress.tranches[nextTranche].sceneIntentTouched = true;
            progress.tranches[nextTranche].sceneIntentSyncPending = true;
          }

          // ── Contract code review (uses merged accumulated content) ───
          const mergedFilesForReview = trancheFilesUpdated.map(p => ({
            path: p,
            content: accumulatedFiles[p] || ""
          }));
          const contractCodeReview = buildContractCodeReviewForTranche(
            progress.tranches[nextTranche],
            mergedFilesForReview,
            approvedRosterBlock
          );
          progress.tranches[nextTranche].contractCodeReviewWarnings = contractCodeReview.warnings;
          progress.tranches[nextTranche].contractCodeReviewStatus = contractCodeReview.status;
          progress.tranches[nextTranche].contractCodeReviewAssets = contractCodeReview.assets;
          progress.contractCodeReview = summarizeContractCodeReview(progress);

          if (contractCodeReview.warnings.length > 0) {
            console.warn(`[CONTRACT CODE REVIEW][informational] Tranche ${nextTranche + 1}: ${contractCodeReview.warnings.join(" || ")}`);
          }

          // Update progress: tranche complete
          progress.tranches[nextTranche].status = "complete";
          progress.tranches[nextTranche].endTime = Date.now();
          progress.tranches[nextTranche].message = trancheResult.message || "Tranche completed.";
          progress.tranches[nextTranche].filesUpdated = trancheFilesUpdated;
          progress.tranches[nextTranche].patchMode = Boolean(trancheResult.isPatch);
          await saveProgress(bucket, projectPath, progress);

          console.log(`Tranche ${nextTranche + 1} complete: ${trancheFilesUpdated.length} file(s) touched (${trancheResult.isPatch ? "patch" : "legacy"} mode).`);

          // ── Checkpoint ai_response.json after every successful merge ──
          if (allUpdatedFiles.length > 0) {
            await saveAiResponse(bucket, projectPath, allUpdatedFiles, {
              jobId:         jobId,
              trancheIndex:  nextTranche,
              totalTranches: progress.totalTranches,
              status:        "checkpoint",
              message:       `Checkpoint after tranche ${nextTranche + 1}/${progress.totalTranches}: ${trancheResult.message || "completed."}`,
              sceneIntentSyncRequired: Boolean(state.sceneIntentSyncState?.staleCompilerOwnedJson),
              sceneIntentSyncState: state.sceneIntentSyncState || null
            });
          }
        }
      }

      // ── Save updated pipeline state ──────────────────────────
      state.progress = progress;
      state.accumulatedFiles = accumulatedFiles;
      state.allUpdatedFiles = allUpdatedFiles;
      state.contractCodeReview = progress.contractCodeReview;
      state.sceneIntentSyncState = state.sceneIntentSyncState || sceneIntentSyncState || {
        staleCompilerOwnedJson: false,
        lastSceneIntentTrancheIndex: null,
        lastSceneIntentTrancheName: null,
        lastSceneIntentTimestamp: null
      };
      await savePipelineState(bucket, projectPath, state);

      // ── Chain to next tranche OR finalize ─────────────────────
      if (nextTranche + 1 < progress.totalTranches) {
        await chainToSelf({
          projectPath,
          jobId,
          mode: "tranche",
          nextTranche: nextTranche + 1
        });
        return { statusCode: 200, body: JSON.stringify({ success: true, chained: true, phase: `tranche_${nextTranche}_complete` }) };
      }

      // ══════════════════════════════════════════════════════════
      //  FINAL — All tranches done, assemble and save response
      // ══════════════════════════════════════════════════════════

      const summaryParts = progress.tranches
        .filter(t => t.status === "complete")
        .map((t) => `Tranche ${t.index + 1} — ${t.name}: ${t.message}`);

      const finalMessage = summaryParts.join("\n\n") || "Build completed.";

      await saveAiResponse(bucket, projectPath, allUpdatedFiles, {
        jobId:         jobId,
        trancheIndex:  progress.totalTranches - 1,
        totalTranches: progress.totalTranches,
        status:        "final",
        message:       finalMessage,
        sceneIntentSyncRequired: Boolean(state.sceneIntentSyncState?.staleCompilerOwnedJson),
        sceneIntentSyncState: state.sceneIntentSyncState || null
      });

      progress.status = "complete";
      const t = progress.tokenUsage.totals;
      progress.finalMessage = `Build complete: ${allUpdatedFiles.length} file(s) updated across ${progress.tranches.filter(tr => tr.status === "complete").length} tranche(s). Tokens: ${t.input_tokens} in / ${t.output_tokens} out.`;
      progress.completedTime = Date.now();
      await saveProgress(bucket, projectPath, progress);

      console.log(`Total tokens — input: ${t.input_tokens}, output: ${t.output_tokens}`);

      // Clean up pipeline state and request files
      try { await bucket.file(`${projectPath}/ai_pipeline_state.json`).delete(); } catch (e) {}
      try { await bucket.file(`${projectPath}/ai_request.json`).delete(); } catch (e) {}

      return { statusCode: 200, body: JSON.stringify({ success: true, phase: "complete" }) };
    }


        // NOTE: "patch_issue" mode has been moved to the synchronous function
    // netlify/functions/claudeCodePatch.js — it cannot return an inline
    // HTTP response from a background function (Netlify returns 202 immediately).

    throw new Error(`Unknown mode: ${mode}`);

  } catch (error) {
    console.error("Claude Code Proxy Background Error:", error);
    try {
      if (projectPath && bucket) {
        await bucket.file(`${projectPath}/ai_error.json`).save(
          JSON.stringify({ error: error.message }),
          { contentType: "application/json", resumable: false }
        );
        try {
          await saveProgress(bucket, projectPath, {
            jobId: jobId || "unknown",
            status: "error",
            error: error.message,
            completedTime: Date.now()
          });
        } catch (e2) {}
      }
    } catch (e) {
      console.error("CRITICAL: Failed to write error to Firebase.", e);
    }

    // Return 202, not 500. Netlify background functions that return any
    // synchronous non-202 status suppress the entire log stream.
    return { statusCode: 202, body: JSON.stringify({ accepted: true }) };
  }
};