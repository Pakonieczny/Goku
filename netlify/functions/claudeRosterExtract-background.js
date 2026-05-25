/* netlify/functions/claudeRosterExtract-background.js */
/* ═══════════════════════════════════════════════════════════════════
   ASSET ROSTER EXTRACTION & FIREBASE STAGING — v3.0
   ─────────────────────────────────────────────────────────────────
   Background Netlify function (suffix -background = 15-min timeout)
   called after the user approves the game-specific Asset Roster.

   Flow:
     1. Load the approved roster from ai_asset_roster_pending.json.
        Each 3D object carries colormapEntryPath + colormapFile locked
        at index time in StageAB — no colormap re-discovery happens here.
     2. For each selected asset, find the matching .zip — searches project-local particle/3D folders
        first, then falls back to the global game-generator-1/projects/BASE_Files/asset_3d_objects/ mega-zips.
        (matched by sourceRosterDocument name → same base name .zip)
     3. Extract only the approved files from each zip (parallel uploads).
        For 3D assets: read colormap from the locked colormapEntryPath and
        stage it renamed to match the model basename (e.g. fountain-center.jpg or avatar__POOL_TABLE.jpg).
        If colormapEntryPath is missing from the zip, log a hard error — no fallback.
     4. Upload extracted files to a game-specific staged folder:
        ${projectPath}/staged_assets/${jobId}/
     5. Save ai_asset_roster_approved.json with staged file paths
     6. assets.json registration is handled by the frontend in two steps
        after this function returns:
          a. copyRosterAssetsToModels() — copies staged files into models/
          b. syncAssetsJson() — scans models/ and rebuilds assets.json;
             approved 3D objects register as children of the Models folder (key "15"),
             approved particle textures register at root level with their own assigned numeric keys.
        This function does NOT write assets.json.
     7. Return { success:true, stagedAssets, stagedFolder, extractionLog }

   Request body:  { projectPath, jobId }
   Response body: { success:true, stagedAssets, stagedFolder, extractionLog }
               |  { success:false, error: "..." }

   NOTE: Renamed to *-background.js so Netlify gives this function a
   15-minute execution window instead of the default 10-second limit.
   ═══════════════════════════════════════════════════════════════════ */

const admin  = require("./firebaseAdmin");
const JSZip  = require("jszip");


/* ─── Helpers ────────────────────────────────────────────────── */
function err400(msg) { return { statusCode: 400, body: JSON.stringify({ success: false, error: msg }) }; }

/* "Nature Pack.docx" → "Nature Pack.zip" */
function zipNameFromRoster(rosterDocName = "") {
  return rosterDocName.replace(/\.docx$/i, ".zip");
}

/* Normalize for fuzzy filename matching:
   strips everything except alphanumerics, dots, dashes, underscores.
   "Tree Trunk.obj" → "treetrunk.obj"
   Used for both the roster asset name and the zip entry name so
   spaces / casing differences do not cause silent misses.           */
function normalizeAssetName(name = "") {
  return name.toLowerCase().replace(/[^a-z0-9.\-_]/g, "");
}

/* Zip content classifier for staged 3D assets */
function getZipEntryBaseName(name = "") {
  const base = String(name || "").split("/").pop() || String(name || "");
  return base.replace(/\.[^.]+$/, "");
}

function detectMimeType(filename = "") {
  const ext = String(filename || "").split(".").pop().toLowerCase();
  return ext === "png"                    ? "image/png"
       : ext === "jpg" || ext === "jpeg" ? "image/jpeg"
       : ext === "webp"                   ? "image/webp"
       : ext === "bmp"                    ? "image/bmp"
       : ext === "tga"                    ? "image/x-targa"
       : ext === "obj"                    ? "text/plain"
       : ext === "fbx"                    ? "model/fbx"
       : ext === "glb" || ext === "gltf" ? "model/gltf-binary"
       : "application/octet-stream";
}

function isSkippableZipEntry(entryPath = "") {
  const normalized = String(entryPath || "").replace(/\\/g, "/");
  const base = normalized.split("/").pop() || "";
  return normalized.includes("__MACOSX") || base.startsWith("._");
}

function isModelAssetName(name = "") {
  return /\.(obj|fbx|glb|gltf|c3b)$/i.test(String(name || ""));
}

function stripExtension(name = "") {
  return String(name || "").replace(/\.[^.]+$/, "");
}

function buildAssetIdentityKey(asset = {}) {
  const assetName = normalizeAssetName(asset?.assetName || "");
  const sourceName = normalizeAssetName(asset?.sourceRosterDocument || asset?.sourceZip || "");
  return `${assetName}::${sourceName}`;
}

function buildCopiedModelFilename(asset = {}, entryPath = "", copiedModelFilenameByAsset = null) {
  if (copiedModelFilenameByAsset instanceof Map && copiedModelFilenameByAsset.has(asset)) {
    return copiedModelFilenameByAsset.get(asset);
  }
  const originalName = String(asset.copiedModelFilename || asset.assetName || entryPath.split("/").pop() || "").trim();
  const extMatch = (entryPath.split("/").pop() || originalName).match(/(\.[^.]+)$/);
  const ext = extMatch ? extMatch[1] : (originalName.match(/(\.[^.]+)$/)?.[1] || "");
  const baseNoExt = stripExtension(originalName || entryPath.split("/").pop() || "asset");
  const isAvatar = Boolean(asset.avatarRole || asset.animationManifestPath || asset.fbxEntryPath || /avatars?\.zip$/i.test(String(asset.sourceZip || "")));
  return isAvatar ? `avatar__${baseNoExt}${ext}` : `${baseNoExt}${ext}`;
}

function buildDeterministicCopiedModelFilenamePlan(allSelected = []) {
  const duplicateModelNameCounts = new Map();
  allSelected.filter(asset => isModelAssetName(asset?.assetName || "")).forEach((asset) => {
    const originalName = String(asset.copiedModelFilename || asset.assetName || "").trim();
    const key = normalizeAssetName(originalName || "asset");
    duplicateModelNameCounts.set(key, Number(duplicateModelNameCounts.get(key) || 0) + 1);
  });

  const duplicateModelNameOrdinals = new Map();
  const copiedModelFilenameByAsset = new Map();
  const seenFilenames = new Map();

  allSelected.filter(asset => isModelAssetName(asset?.assetName || "")).forEach((asset) => {
    const originalName = String(asset.copiedModelFilename || asset.assetName || "").trim();
    const extMatch = originalName.match(/(\.[^.]+)$/);
    const ext = extMatch ? extMatch[1] : "";
    const baseNoExt = stripExtension(originalName || "asset");
    const normalizedKey = normalizeAssetName(originalName || "asset");
    const duplicateCount = Number(duplicateModelNameCounts.get(normalizedKey) || 0);
    const nextOrdinal = Number(duplicateModelNameOrdinals.get(normalizedKey) || 0) + 1;
    duplicateModelNameOrdinals.set(normalizedKey, nextOrdinal);
    const suffix = String(nextOrdinal).padStart(2, '0');
    const isAvatar = Boolean(asset.avatarRole || asset.animationManifestPath || asset.fbxEntryPath || /avatars?\.zip$/i.test(String(asset.sourceZip || "")));

    let copiedModelFilename;
    if (isAvatar) {
      copiedModelFilename = duplicateCount > 1
        ? `avatar__${baseNoExt}__${suffix}${ext}`
        : `avatar__${baseNoExt}${ext}`;
    } else if (duplicateCount > 1) {
      copiedModelFilename = `prop__${baseNoExt}__${suffix}${ext}`;
    } else {
      copiedModelFilename = `${baseNoExt}${ext}`;
    }

    const normalizedFilename = normalizeAssetName(copiedModelFilename);
    if (seenFilenames.has(normalizedFilename)) {
      const priorAsset = seenFilenames.get(normalizedFilename);
      throw new Error(
        `Duplicate copiedModelFilename assignment detected: ${copiedModelFilename} for ${priorAsset?.assetName || '(unknown asset)'} and ${asset?.assetName || '(unknown asset)'}`
      );
    }
    seenFilenames.set(normalizedFilename, asset);
    copiedModelFilenameByAsset.set(asset, copiedModelFilename);
  });

  return copiedModelFilenameByAsset;
}

/* ═══════════════════════════════════════════════════════════════ */
exports.handler = async (event) => {
  let projectPath = null;
  let jobId = null;

  try {
    if (!event.body) return err400("Missing request body");

    const body = JSON.parse(event.body);
    projectPath = body.projectPath;
    jobId = body.jobId;
    const avatarZipPath = body.avatarPipeline?.zipPath || '';
    if (!projectPath) return err400("Missing projectPath");
    if (!jobId)       return err400("Missing jobId");
    if (!avatarZipPath) return err400("Missing avatarPipeline.zipPath");

    const bucket = admin.storage().bucket(
      process.env.FIREBASE_STORAGE_BUCKET || "gokudatabase.firebasestorage.app"
    );

    console.log(`[ROSTER-EXTRACT] Starting extraction — project: ${projectPath}, job: ${jobId}`);

    // ── 1. Load the pending roster ───────────────────────────────────
    const pendingFile = bucket.file(`${projectPath}/ai_asset_roster_pending.json`);
    const [pendingExists] = await pendingFile.exists();
    if (!pendingExists) return err400("ai_asset_roster_pending.json not found. Run roster generation first.");

    const [pendingContent] = await pendingFile.download();
    const roster = JSON.parse(pendingContent.toString());

    const objects3d   = Array.isArray(roster.objects3d)     ? roster.objects3d     : [];
    const textures    = Array.isArray(roster.textureAssets) ? roster.textureAssets : [];
    const avatars     = Array.isArray(roster.avatars)       ? roster.avatars       : [];
    const allSelected = [...objects3d, ...textures, ...avatars];

    const copiedModelFilenameByAsset = buildDeterministicCopiedModelFilenamePlan(allSelected);

    if (allSelected.length === 0) {
      return err400("No assets selected in the roster. Cannot proceed with extraction.");
    }

    // ── 2. Group selected assets by source zip ───────────────────────
    const byZip = new Map(); // zipName → [asset objects]
    for (const asset of allSelected) {
      const zipName = asset.sourceZip || zipNameFromRoster(asset.sourceRosterDocument || "");
      if (!zipName || zipName === ".zip") {
        console.warn(`[ROSTER-EXTRACT] Asset "${asset.assetName}" has no valid sourceZip or sourceRosterDocument — skipping.`);
        continue;
      }
      if (!byZip.has(zipName)) byZip.set(zipName, []);
      byZip.get(zipName).push(asset);
    }

    // ── 3. List available zip files ─────────────────────────────────
    // Search order:
    //   1. ${projectPath}/asset_particle_textures/  — project-local particle zips
    //   2. ${projectPath}/asset_3d_objects/          — project-local 3D zips (legacy / future)
    //   3. game-generator-1/projects/BASE_Files/asset_3d_objects/    — global mega-zips (Architecture_Modular.zip etc.)
    //
    // Project-local paths are checked first so a project-specific override
    // can shadow a global zip of the same name if needed.
    // Later entries do NOT overwrite earlier ones in availableZips — first match wins.
    const GLOBAL_ASSET_BASE = "game-generator-1/projects/BASE_Files/asset_3d_objects";
    const LEGACY_AVATAR_ZIP_FOLDER = "game-generator-1/projects/BASE_Files/avatar_assets/";
    const AVATAR_ZIP_FOLDER = avatarZipPath.replace(/[^/]+$/, '');
    const availableZips = new Map(); // lowercased base filename → bucket File reference
    const zipSearchFolders = [
      `${projectPath}/asset_particle_textures/`,
      `${projectPath}/asset_3d_objects/`,
      `${GLOBAL_ASSET_BASE}/`,
      AVATAR_ZIP_FOLDER,
      LEGACY_AVATAR_ZIP_FOLDER
    ];
    for (const folder of zipSearchFolders) {
      let folderFiles;
      try {
        [folderFiles] = await bucket.getFiles({ prefix: folder });
      } catch (e) {
        console.warn(`[ROSTER-EXTRACT] Could not list folder ${folder}: ${e.message}`);
        continue;
      }
      for (const f of folderFiles || []) {
        const base = f.name.split("/").pop();
        if (!base || !base.toLowerCase().endsWith(".zip")) continue;
        const key = base.toLowerCase();
        if (!availableZips.has(key)) {
          // First match wins — project-local overrides global if names collide
          availableZips.set(key, f);
        }
      }
    }
    console.log(`[ROSTER-EXTRACT] Zip index built: ${availableZips.size} zip(s) found across all folders`);

    // ── 4. Extract and stage assets ──────────────────────────────────
    const stagedFolderPath = `${projectPath}/staged_assets/${jobId}`;
    const stagedAssets  = [];
    const extractionLog = [];

    for (const [zipName, assets] of byZip.entries()) {
      const zipFile = availableZips.get(zipName.toLowerCase());
      if (!zipFile) {
        console.warn(`[ROSTER-EXTRACT] Zip not found in any search folder (project-local or global): ${zipName}`);
        extractionLog.push({ zipName, status: "missing", assetCount: assets.length });
        continue;
      }

      // Download and parse the zip
      const [zipBuffer] = await zipFile.download();
      let zip;
      try {
        zip = await JSZip.loadAsync(zipBuffer);
      } catch (e) {
        console.warn(`[ROSTER-EXTRACT] Could not parse zip ${zipName}: ${e.message}`);
        extractionLog.push({ zipName, status: "parse_error", error: e.message });
        continue;
      }

      // Build normalized lookup: normalizedBaseName → full zip entry path
      const zipEntries = new Map();
      for (const entryPath of Object.keys(zip.files)) {
        const entry = zip.files[entryPath];
        if (entry.dir || isSkippableZipEntry(entryPath)) continue;
        const baseName = entryPath.split("/").pop();
        zipEntries.set(normalizeAssetName(baseName), entryPath);
      }

      // Upload all approved assets from this zip in parallel.
      // Colormap matching was locked at index time in StageAB — asset.colormapEntryPath
      // is the authoritative full zip path. No re-discovery happens here.
      const uploadTasks = assets.map(async (asset) => {
        const normalizedTarget = normalizeAssetName(asset.assetName);
        const explicitEntryPath = asset.fbxEntryPath || asset.assetEntryPath || asset.entryPath || null;
        const entryPath = (explicitEntryPath && zip.files[explicitEntryPath] && !zip.files[explicitEntryPath].dir)
                       ? explicitEntryPath
                       : zipEntries.get(normalizedTarget)
                       || zipEntries.get(normalizedTarget + '.obj')
                       || zipEntries.get(normalizedTarget + '.fbx')
                       || zipEntries.get(normalizedTarget + '.glb')
                       || zipEntries.get(normalizedTarget + '.gltf');

        if (!entryPath) {
          const availableSample = [...zipEntries.keys()].slice(0, 10).join(", ");
          const msg = `"${asset.assetName}" (normalized: "${normalizedTarget}") not found in ${zipName}. ` +
                      `Sample available: [${availableSample}]`;
          console.warn(`[ROSTER-EXTRACT] ${msg}`);
          extractionLog.push({ zipName, asset: asset.assetName, status: "not_in_zip", detail: msg });
          return null;
        }

        try {
          const originalEntryName = entryPath.split('/').pop() || asset.assetName;
          const copiedModelFilename = isModelAssetName(originalEntryName)
            ? buildCopiedModelFilename(asset, entryPath, copiedModelFilenameByAsset)
            : null;
          const stagedFilename = copiedModelFilename || asset.assetName || originalEntryName;
          const fileData = await zip.files[entryPath].async("nodebuffer");
          const stagedPath = `${stagedFolderPath}/${stagedFilename}`;
          await bucket.file(stagedPath).save(fileData, {
            contentType: detectMimeType(stagedFilename),
            resumable: false
          });

          const is3dAsset = isModelAssetName(originalEntryName || asset.assetName || '');
          const isAvatarAsset = Boolean(asset.avatarRole || asset.animationManifestPath || asset.fbxEntryPath || /avatars?\.zip$/i.test(String(asset.sourceZip || '')));
          let colormapFile       = null;
          let colormapStagedPath = null;
          let colormapConfidence = "NONE";
          let colormapDetectionRule = "none";
          let stagedTextureFiles = [];
          let stagedTexturePaths = [];
          let stagedAnimationManifestFile = null;
          let stagedAnimationManifestPath = null;

          if (is3dAsset) {
            if (asset.colormapEntryPath) {
              if (!zip.files[asset.colormapEntryPath]) {
                console.error(`[ROSTER-EXTRACT] Locked colormapEntryPath "${asset.colormapEntryPath}" not found in ${zipName} for "${asset.assetName}". Roster may be stale.`);
                extractionLog.push({ zipName, asset: asset.assetName, status: "colormap_locked_path_missing", detail: asset.colormapEntryPath });
              } else {
                const colormapBuffer = await zip.files[asset.colormapEntryPath].async("nodebuffer");
                const assetBase       = getZipEntryBaseName(copiedModelFilename || asset.assetName);
                const rawColormapName = asset.colormapEntryPath.split("/").pop();
                const colormapExt     = rawColormapName.includes(".")
                  ? rawColormapName.slice(rawColormapName.lastIndexOf("."))
                  : "";
                const stagedColormapName = `${assetBase}${colormapExt}`;

                colormapFile          = stagedColormapName;
                colormapStagedPath    = `${stagedFolderPath}/${stagedColormapName}`;
                colormapConfidence    = asset.colormapConfidence || "HIGH";
                colormapDetectionRule = asset.colormapDetectionRule || "locked-at-index";

                await bucket.file(colormapStagedPath).save(colormapBuffer, {
                  contentType: detectMimeType(rawColormapName),
                  resumable: false
                });
                console.log(`[ROSTER-EXTRACT] Colormap staged for "${asset.assetName}": ${stagedColormapName} (${colormapDetectionRule})`);
              }
            } else if (!isAvatarAsset || !Array.isArray(asset.textureFiles) || asset.textureFiles.length === 0) {
              console.warn(`[ROSTER-EXTRACT] No colormapEntryPath for "${asset.assetName}" — no colormap was found in its zip folder during StageAB indexing.`);
              extractionLog.push({ zipName, asset: asset.assetName, status: "colormap_not_found", detail: "No colormap detected in asset folder during StageAB indexing" });
            }
          }

          if (isAvatarAsset) {
            const bindingContract = Array.isArray(asset.textureBindingContract)
              ? asset.textureBindingContract
              : [];
            const boundEntryPaths = bindingContract
              .filter(b => b.boundTexture && b.confidence !== 'unresolved')
              .map(b => b.boundTexture);
            const texturesToStage = boundEntryPaths.length > 0
              ? boundEntryPaths
              : (Array.isArray(asset.textureFiles) ? asset.textureFiles : []);
            for (const textureRef of texturesToStage) {
              const textureEntryPath = typeof textureRef === 'string' ? textureRef : (textureRef?.entryPath || textureRef?.path || textureRef?.name || '');
              if (!textureEntryPath || !zip.files[textureEntryPath] || isSkippableZipEntry(textureEntryPath)) continue;
              const rawTextureName = textureEntryPath.split('/').pop();
              // buildDeterministicCopiedModelFilenamePlan already prefixes avatar mesh
              // filenames with "avatar__". Re-prefixing here produced doubled
              // "avatar__avatar__<base>__<texture>" names that didn't match anything in
              // models/ after copy, causing all four `avatar__avatar__...` RENAME-TO-KEY
              // failures in the error log. Strip the existing prefix before re-applying.
              const textureBase = stripExtension(copiedModelFilename || asset.assetName || originalEntryName).replace(/^avatar__/i, '');
              const stagedTextureName = `avatar__${textureBase}__${rawTextureName}`;
              const stagedTexturePath = `${stagedFolderPath}/${stagedTextureName}`;
              const textureBuffer = await zip.files[textureEntryPath].async('nodebuffer');
              await bucket.file(stagedTexturePath).save(textureBuffer, {
                contentType: detectMimeType(rawTextureName),
                resumable: false
              });
              stagedTextureFiles.push(stagedTextureName);
              stagedTexturePaths.push(stagedTexturePath);
            }

            const animationManifestEntryPath = asset.animationManifestPath || null;
            if (animationManifestEntryPath && zip.files[animationManifestEntryPath] && !isSkippableZipEntry(animationManifestEntryPath)) {
              const rawManifestName = animationManifestEntryPath.split('/').pop() || 'Animations.txt';
              // Same double-prefix guard as the texture path above.
              const manifestBase = stripExtension(copiedModelFilename || asset.assetName || originalEntryName).replace(/^avatar__/i, '');
              stagedAnimationManifestFile = `avatar__${manifestBase}__${rawManifestName}`;
              stagedAnimationManifestPath = `${stagedFolderPath}/${stagedAnimationManifestFile}`;
              const manifestBuffer = await zip.files[animationManifestEntryPath].async('nodebuffer');
              await bucket.file(stagedAnimationManifestPath).save(manifestBuffer, {
                contentType: 'text/plain',
                resumable: false
              });
            }
          }

          return {
            assetName:            asset.assetName,
            sourceRosterDocument: asset.sourceRosterDocument,
            stagedPath,
            copiedModelFilename,
            colormapFile,
            colormapStagedPath,
            colormapConfidence,
            colormapDetectionRule,
            stagedTextureFiles,
            stagedTexturePaths,
            stagedAnimationManifestFile,
            stagedAnimationManifestPath,
            // Carry fbxEntryPath through so the frontend copyRosterAssetsToModels
            // warning guard can distinguish avatar-path FBX assets from prop-library
            // objects. Avatar-path assets may now stage BOTH a locked colormap and
            // stagedTexturePaths when both are present.
            fbxEntryPath:         asset.fbxEntryPath || null,
            intendedRole:         asset.intendedRole || asset.intendedUsage || '',
            selectionRationale:   asset.selectionRationale || ''
          };
        } catch (e) {
          console.warn(`[ROSTER-EXTRACT] Upload failed for ${asset.assetName}: ${e.message}`);
          extractionLog.push({ zipName, asset: asset.assetName, status: "upload_error", error: e.message });
          return null;
        }
      });

      const results   = await Promise.all(uploadTasks);
      const succeeded = results.filter(Boolean);
      stagedAssets.push(...succeeded);

      extractionLog.push({ zipName, status: "ok", extracted: succeeded.length, attempted: assets.length });
      console.log(`[ROSTER-EXTRACT] ${zipName}: ${succeeded.length}/${assets.length} asset(s) staged`);
    }

    // ── 5. Save approved roster with staged paths ────────────────────
    const stagedQueues = new Map();
    const stagedQueuesByAssetName = new Map();
    stagedAssets
      .filter(asset => asset?.assetName)
      .forEach((asset) => {
        const identityKey = buildAssetIdentityKey(asset);
        if (!stagedQueues.has(identityKey)) stagedQueues.set(identityKey, []);
        stagedQueues.get(identityKey).push(asset);

        const assetNameKey = normalizeAssetName(asset.assetName);
        if (!stagedQueuesByAssetName.has(assetNameKey)) stagedQueuesByAssetName.set(assetNameKey, []);
        stagedQueuesByAssetName.get(assetNameKey).push(asset);
      });

    const enrichApprovedAsset = (asset) => {
      const identityKey = buildAssetIdentityKey(asset);
      const identityQueue = stagedQueues.get(identityKey);
      let stagedMeta = Array.isArray(identityQueue) && identityQueue.length > 0 ? identityQueue.shift() : null;
      if (!stagedMeta) {
        const assetNameKey = normalizeAssetName(asset?.assetName || "");
        const queue = stagedQueuesByAssetName.get(assetNameKey);
        stagedMeta = Array.isArray(queue) && queue.length > 0 ? queue.shift() : null;
      }
      if (!stagedMeta) return asset;
      return {
        ...asset,
        stagedPath: stagedMeta.stagedPath || asset.stagedPath || null,
        copiedModelFilename: stagedMeta.copiedModelFilename || asset.copiedModelFilename || null,
        colormapFile: stagedMeta.colormapFile || asset.colormapFile || null,
        colormapStagedPath: stagedMeta.colormapStagedPath || asset.colormapStagedPath || null,
        colormapConfidence: stagedMeta.colormapConfidence || asset.colormapConfidence || "NONE",
        colormapDetectionRule: stagedMeta.colormapDetectionRule || asset.colormapDetectionRule || "none",
        stagedTextureFiles: Array.isArray(stagedMeta.stagedTextureFiles) && stagedMeta.stagedTextureFiles.length > 0 ? stagedMeta.stagedTextureFiles : (asset.stagedTextureFiles || []),
        stagedTexturePaths: Array.isArray(stagedMeta.stagedTexturePaths) && stagedMeta.stagedTexturePaths.length > 0 ? stagedMeta.stagedTexturePaths : (asset.stagedTexturePaths || []),
        stagedAnimationManifestFile: stagedMeta.stagedAnimationManifestFile || asset.stagedAnimationManifestFile || null,
        stagedAnimationManifestPath: stagedMeta.stagedAnimationManifestPath || asset.stagedAnimationManifestPath || null
      };
    };

    const approvedRoster = {
      ...roster,
      objects3d: (roster.objects3d || []).map(enrichApprovedAsset),
      textureAssets: (roster.textureAssets || []).map(enrichApprovedAsset),
      avatars: (roster.avatars || []).map(enrichApprovedAsset),
      _meta: {
        ...roster._meta,
        approved:         true,
        approvedAt:       Date.now(),
        stagedFolder:     stagedFolderPath,
        stagedAssetCount: stagedAssets.length,
        extractionLog
      },
      stagedAssets
    };

    await bucket.file(`${projectPath}/ai_asset_roster_approved.json`).save(
      JSON.stringify(approvedRoster, null, 2),
      { contentType: "application/json", resumable: false }
    );

    // Mark pending as approved (UI reference only)
    roster._meta = { ...roster._meta, approved: true, approvedAt: Date.now() };
    await bucket.file(`${projectPath}/ai_asset_roster_pending.json`).save(
      JSON.stringify(roster, null, 2),
      { contentType: "application/json", resumable: false }
    );

    // ── 6. assets.json is NOT written here ───────────────────────────
    // The frontend handles manifest registration in two steps after this
    // function returns: copyRosterAssetsToModels() copies staged files into
    // models/, then syncAssetsJson() rebuilds assets.json — approved 3D objects
    // register as children of the Models folder (key "15"), while approved
    // particle textures register at root level with their own assigned numeric keys.

    // ── 7. Write result sentinel — frontend polls for this file ──────
    // Background functions return 202 immediately with no body. The frontend
    // polls ai_asset_roster_extract_result.json until it appears with a
    // completedAt timestamp matching this jobId, then reads the staged assets.
    const resultPayload = {
      success:      true,
      jobId,
      completedAt:  Date.now(),
      stagedAssets,
      stagedFolder: stagedFolderPath,
      extractionLog
    };
    await bucket.file(`${projectPath}/ai_asset_roster_extract_result.json`)
      .save(JSON.stringify(resultPayload, null, 2), { contentType: "application/json", resumable: false });

    console.log(`[ROSTER-EXTRACT] Complete. ${stagedAssets.length} asset(s) staged to ${stagedFolderPath}`);

    // Background function — response body is ignored by Netlify, but return cleanly.
    return { statusCode: 200, body: "" };

  } catch (error) {
    console.error("[ROSTER-EXTRACT] Unhandled error:", error);

    // Write error sentinel so the frontend poller surfaces the failure immediately
    // instead of timing out after 15 minutes.
    try {
      if (projectPath) {
        const bucket = admin.storage().bucket(
          process.env.FIREBASE_STORAGE_BUCKET || "gokudatabase.firebasestorage.app"
        );
        await bucket.file(`${projectPath}/ai_asset_roster_extract_error.json`)
          .save(JSON.stringify({ success: false, jobId, error: error.message, failedAt: Date.now() }),
                { contentType: "application/json", resumable: false });
      }
    } catch (writeErr) {
      console.warn("[ROSTER-EXTRACT] Could not write error sentinel:", writeErr.message);
    }

    return { statusCode: 500, body: "" };
  }
};

