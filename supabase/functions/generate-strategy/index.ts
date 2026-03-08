import "jsr:@supabase/functions-js/edge-runtime.d.ts";
const SUPABASE_URL = Deno.env.get('SUPABASE_URL')!, SUPABASE_KEY = Deno.env.get('SUPABASE_SERVICE_ROLE_KEY')!;
const ANTHROPIC_KEY = Deno.env.get('ANTHROPIC_API_KEY');
const BD_TOKEN = Deno.env.get('BRIGHT_DATA_TOKEN');
const PAPPERS_KEY = Deno.env.get('PAPPERS_API_KEY');
const CLAUDE_MODEL = 'claude-sonnet-4-5-20250929';
const SB_H = { 'apikey': SUPABASE_KEY, 'Authorization': 'Bearer ' + SUPABASE_KEY, 'Content-Type': 'application/json' };
const BD_H = { 'Authorization': 'Bearer ' + BD_TOKEN, 'Content-Type': 'application/json' };
const DS_LI_PERSON = 'gd_l1viktl72bvl7bjuj0', DS_LI_COMPANY = 'gd_l1vikfnt1wgvvqz95w', DS_LI_POSTS = 'gd_lyy3tktm25m4avu764';
let _claudeDiag: {raw_len: number, stop: string, in_tok: number, out_tok: number, err?: string, preview?: string} | null = null;

async function sb(path: string, opts: any = {}) { const r = await fetch(SUPABASE_URL + '/rest/v1/' + path, { headers: SB_H, ...opts }); if (!r.ok) { console.log('SB err ' + r.status + ' ' + path.split('?')[0]); return null; } const t = await r.text(); return t ? JSON.parse(t) : null; }

async function bdDataset(dsId: string, input: any[], timeout = 55000): Promise<any> {
  const label = dsId === DS_LI_PERSON ? 'person' : dsId === DS_LI_COMPANY ? 'company' : dsId === DS_LI_POSTS ? 'posts' : dsId;
  try {
    console.log('BD[' + label + '] trigger url=' + JSON.stringify(input).slice(0, 200));
    const tr = await fetch('https://api.brightdata.com/datasets/v3/trigger?dataset_id=' + dsId + '&format=json&include_errors=true', { method: 'POST', headers: BD_H, body: JSON.stringify(input) });
    if (!tr.ok) { const tb = await tr.text(); console.log('BD[' + label + '] trigger fail:' + tr.status + ' body=' + tb.slice(0, 300)); return null; }
    const td = await tr.json(); if (!td?.snapshot_id) { console.log('BD[' + label + '] no snapshot, resp=' + JSON.stringify(td).slice(0, 300)); return null; }
    console.log('BD[' + label + '] snapshot=' + td.snapshot_id);
    const t0 = Date.now(); let pollDelay = 2000; let pollCount = 0;
    while (Date.now() - t0 < timeout) {
      await new Promise(r => setTimeout(r, pollDelay));
      pollDelay = Math.min(pollDelay * 1.5, 8000); pollCount++;
      const sr = await fetch('https://api.brightdata.com/datasets/v3/snapshot/' + td.snapshot_id + '?format=json', { headers: BD_H });
      if (!sr.ok) { console.log('BD[' + label + '] poll#' + pollCount + ' HTTP ' + sr.status); continue; }
      const b = await sr.text();
      if (b.includes('"status":"running"')) { continue; }
      console.log('BD[' + label + '] done in ' + ((Date.now() - t0) / 1000).toFixed(1) + 's polls=' + pollCount + ' body=' + b.slice(0, 400));
      try {
        const parsed = JSON.parse(b);
        // Check if BD returned errors
        if (Array.isArray(parsed) && parsed.length > 0 && parsed[0]?.error) { console.log('BD[' + label + '] returned error: ' + JSON.stringify(parsed[0].error).slice(0, 200)); }
        if (Array.isArray(parsed) && parsed.length === 0) { console.log('BD[' + label + '] returned empty array'); }
        return parsed;
      } catch { console.log('BD[' + label + '] JSON parse fail: ' + b.slice(0, 200)); return null; }
    }
    console.log('BD[' + label + '] TIMEOUT after ' + (timeout / 1000) + 's polls=' + pollCount);
    return null;
  } catch (e) { console.log('BD[' + label + '] err:' + e); return null; }
}

// Scrape main + about/team + MENTIONS LEGALES for SIREN/legal name
async function scrapeWeb(url: string): Promise<{ main: string | null, about: string | null, team: string | null, legal: string | null, legalInfo: { siren?: string, denomination?: string, forme_juridique?: string } | null }> {
  const scrape = async (u: string): Promise<string | null> => {
    try {
      const r = await fetch(u, { headers: { 'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36' }, redirect: 'follow' });
      if (!r.ok) return null; const h = await r.text();
      return h.replace(/<script[^>]*>[\s\S]*?<\/script>/gi, '').replace(/<style[^>]*>[\s\S]*?<\/style>/gi, '').replace(/<[^>]+>/g, ' ').replace(/\s+/g, ' ').trim().slice(0, 5000);
    } catch { return null; }
  };
  const main = await scrape(url);
  const base = url.replace(/\/$/, '');
  // About/team pages
  const aboutPaths = ['/about', '/a-propos', '/qui-sommes-nous', '/about-us', '/team', '/equipe', '/notre-equipe', '/leadership', '/societe', '/company'];
  let about: string | null = null, team: string | null = null;
  for (const p of aboutPaths) {
    const c = await scrape(base + p);
    if (c && c.length > 200) {
      const lo = c.toLowerCase();
      if (lo.match(/directeur|director|vp |head of|chief|fondateur|founder|ceo|cro|coo|cfo|commercial|sales/)) {
        team = c; if (!about) about = c; break;
      }
      if (!about) about = c;
    }
  }
  // LEGAL PAGES: scrape for SIREN + legal entity name
  const legalPaths = ['/mentions-legales', '/legal', '/cgv', '/conditions-generales', '/mentions', '/legal-notice', '/cgu', '/fr/mentions-legales', '/fr/legal-notice', '/imprint'];
  let legal: string | null = null;
  let legalInfo: { siren?: string, denomination?: string, forme_juridique?: string } | null = null;
  for (const p of legalPaths) {
    const c = await scrape(base + p);
    if (c && c.length > 100) {
      legal = c;
      // Extract SIREN (9 digits near keywords)
      const sirenMatch = c.match(/(?:SIREN|RCS|numéro|number|registre|immatricul|sous le)[^0-9]{0,40}(\d{3}\s?\d{3}\s?\d{3})(?!\s?\d)/i);
      if (!sirenMatch) {
        const sirenAlt = c.match(/(\d{3}\s\d{3}\s\d{3})(?!\s?\d)/);
        if (sirenAlt) legalInfo = { ...(legalInfo || {}), siren: sirenAlt[1].replace(/\s/g, '') };
      } else {
        legalInfo = { ...(legalInfo || {}), siren: sirenMatch[1].replace(/\s/g, '') };
      }
      // Extract legal name + form: "NOM SAS/SARL/SA"
      const legalName = c.match(/([A-ZÀ-Ü][A-ZÀ-Ü\w\s&.'-]{0,40}?)\s*,?\s*\b(SAS|SARL|SASU|SCA|EURL)\b/);
      if (legalName) {
        let denom = legalName[1].trim();
        for (const pfx of ['La société ', 'société ', 'Editeur du site ', 'EDITEUR DU SITE ', 'édité par ', 'Edité par ', 'fully owned by ', 'by ', 'par ']) {
          if (denom.toLowerCase().startsWith(pfx.toLowerCase())) denom = denom.slice(pfx.length).trim();
        }
        if (denom.length >= 2) legalInfo = { ...(legalInfo || {}), denomination: denom, forme_juridique: legalName[2].toUpperCase() };
      }
      if (!legalName) {
        const saMatch = c.match(/([A-ZÀ-Ü][A-ZÀ-Ü\w\s&.'-]{1,40}?)\s+(SA)\s/);
        if (saMatch) {
          let denom = saMatch[1].trim();
          for (const pfx of ['La société ', 'société ', 'fully owned by ', 'by ', 'par ']) {
            if (denom.toLowerCase().startsWith(pfx.toLowerCase())) denom = denom.slice(pfx.length).trim();
          }
          if (denom.length >= 2) legalInfo = { ...(legalInfo || {}), denomination: denom, forme_juridique: 'SA' };
        }
      }
      if (legalInfo?.siren) break; // Found SIREN, stop searching
    }
  }
  return { main, about, team, legal, legalInfo };
}

const APIFY_TOKEN = Deno.env.get('APIFY_API_TOKEN');

// Helper APIFY
async function runApifyActor(actorId: string, input: any, timeout = 60000): Promise<any[]> {
  if (!APIFY_TOKEN) { console.log('Apify token missing'); return []; }
  try {
    const runRes = await fetch(`https://api.apify.com/v2/acts/${actorId}/runs?token=${APIFY_TOKEN}`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(input)
    });
    if (!runRes.ok) { console.log('Apify run error: ' + runRes.status); return []; }
    const runData = await runRes.json();
    const runId = runData.data.id;
    const defaultDatasetId = runData.data.defaultDatasetId;

    // Wait for completion (adaptive polling)
    const t0 = Date.now(); let apifyDelay = 2000;
    while (Date.now() - t0 < timeout) {
      await new Promise(r => setTimeout(r, apifyDelay));
      apifyDelay = Math.min(apifyDelay * 1.5, 8000);
      const statusRes = await fetch(`https://api.apify.com/v2/actor-runs/${runId}?token=${APIFY_TOKEN}`);
      if (!statusRes.ok) continue;
      const statusData = await statusRes.json();
      if (statusData.data.status === 'SUCCEEDED') {
        const datasetRes = await fetch(`https://api.apify.com/v2/datasets/${defaultDatasetId}/items?token=${APIFY_TOKEN}`);
        if (!datasetRes.ok) return [];
        return await datasetRes.json();
      } else if (statusData.data.status === 'FAILED' || statusData.data.status === 'ABORTED') {
        console.log('Apify run failed: ' + statusData.data.status);
        return [];
      }
    }
    console.log('Apify timeout');
    return [];
  } catch (e) {
    console.log('Apify exception: ' + e);
    return [];
  }
}

async function fetchApifyGoogleSearch(entreprise: string): Promise<any[]> {
  const query = `${entreprise} actualités OR news OR levée OR acquisition OR recrutement OR nomination OR partenariat OR croissance OR innovation`;
  console.log('GoogleSearch query: ' + query);
  const results = await runApifyActor('apify/google-search-scraper', {
    queries: [query],
    maxPagesPerQuery: 1,
    resultsPerPage: 8,
    countryCode: 'fr',
    languageCode: 'fr'
  }, 35000);

  console.log('GoogleSearch raw results: count=' + results.length + ' keys=' + (results[0] ? Object.keys(results[0]).join(',') : 'EMPTY'));
  if (results.length > 0 && !results[0]?.organicResults) {
    console.log('GoogleSearch first result preview: ' + JSON.stringify(results[0]).slice(0, 500));
  }

  const items: any[] = [];
  for (const r of results) {
    if (r.organicResults) {
      for (const org of r.organicResults) {
        items.push({ titre: org.title, date: org.date || '', source: org.url });
        if (items.length >= 8) break;
      }
    }
  }
  console.log('GoogleSearch extracted: ' + items.length + ' items');
  return items;
}

// Apify Web Scraper for Company site
async function fetchApifyWebScrape(url: string): Promise<{ main: string | null, about: string | null, team: string | null, legal: string | null, legalInfo: { siren?: string, denomination?: string, forme_juridique?: string } | null }> {
  // Try to use Apify to scrape the site using Cheerio Scraper to get text content
  const startUrls = [{ url }];
  const base = url.replace(/\/$/, '');
  const extraPaths = ['/about', '/a-propos', '/equipe', '/mentions-legales', '/legal'];
  for (const p of extraPaths) startUrls.push({ url: base + p });

  const results = await runApifyActor('apify/cheerio-scraper', {
    startUrls,
    pageFunction: `async ({ $, request, log }) => { 
      return { url: request.url, title: $('title').text(), text: $('body').text().replace(/\\s+/g, ' ').trim().slice(0, 5000) }; 
    }`,
    maxRequestsPerCrawl: 6
  }, 35000);

  let main: string | null = null, about: string | null = null, team: string | null = null, legal: string | null = null;
  let legalInfo: { siren?: string, denomination?: string, forme_juridique?: string } | null = null;

  for (const r of results) {
    if (!r.text) continue;
    const isMain = r.url === url || r.url === url + '/';
    if (isMain) main = r.text;

    if (r.url.match(/about|propos|societe/i)) about = r.text;
    if (r.url.match(/team|equipe|leadership/i)) { team = r.text; if (!about) about = r.text; }
    if (r.url.match(/legal|mention/i)) {
      legal = r.text;
      // Extract SIREN
      const sirenMatch = r.text.match(/(?:SIREN|RCS|numéro|number|registre)[^0-9]{0,40}(\d{3}\s?\d{3}\s?\d{3})(?!\s?\d)/i);
      if (sirenMatch) legalInfo = { ...(legalInfo || {}), siren: sirenMatch[1].replace(/\s/g, '') };

      const legalName = r.text.match(/([A-ZÀ-Ü][A-ZÀ-Ü\w\s&.'-]{0,40}?)\s*,?\s*\b(SAS|SARL|SASU|SCA|EURL|SA)\b/);
      if (legalName) {
        let denom = legalName[1].trim();
        if (denom.length >= 2) legalInfo = { ...(legalInfo || {}), denomination: denom, forme_juridique: legalName[2].toUpperCase() };
      }
    }
  }

  // Scraper fallback if Apify fails
  if (!main && !about && !legal) {
    console.log('Apify scrape yielded nothing, falling back to native scrape');
    return await scrapeWeb(url);
  }

  return { main, about, team, legal, legalInfo };
}

// Enhanced Pappers: try SIREN first (from legal page), then brand name, then legal name
async function fetchPappers(entreprise: string, siren?: string, legalDenom?: string): Promise<any> {
  try {
    let sirenNum = siren;
    if (!sirenNum) {
      // Try brand name first
      const sr = await fetch('https://api.pappers.fr/v2/recherche?api_token=' + PAPPERS_KEY + '&q=' + encodeURIComponent(entreprise) + '&par_page=1');
      if (sr.ok) { const sd = await sr.json(); sirenNum = sd?.resultats?.[0]?.siren; }
      // If not found and we have legal name, try that
      if (!sirenNum && legalDenom && legalDenom.toLowerCase() !== entreprise.toLowerCase()) {
        console.log('Pappers: brand "' + entreprise + '" not found, trying legal "' + legalDenom + '"');
        const sr2 = await fetch('https://api.pappers.fr/v2/recherche?api_token=' + PAPPERS_KEY + '&q=' + encodeURIComponent(legalDenom) + '&par_page=1');
        if (sr2.ok) { const sd2 = await sr2.json(); sirenNum = sd2?.resultats?.[0]?.siren; }
      }
      if (!sirenNum) return null;
    }
    const er = await fetch('https://api.pappers.fr/v2/entreprise?api_token=' + PAPPERS_KEY + '&siren=' + sirenNum);
    if (!er.ok) return null; const ed = await er.json();
    const fins = (ed.finances || []).slice(0, 5);
    const allDirs = (ed.representants || []).slice(0, 20);
    const keyDirs = allDirs.filter((r: any) => ['Président', 'Directeur général', 'Gérant', 'Directeur général délégué'].includes(r.qualite)).slice(0, 5);
    const bens = (ed.beneficiaires_effectifs || []).slice(0, 5);
    // Build company history timeline
    const historique: any[] = [];
    if (ed.date_creation) historique.push({ date: ed.date_creation, type: 'creation', label: 'Création ' + (ed.denomination || entreprise) });
    for (const d of allDirs) { if (d.date_prise_de_poste) historique.push({ date: d.date_prise_de_poste, type: 'nomination', label: d.qualite + ': ' + (d.prenom || '') + ' ' + (d.nom || '') }); }
    for (const f of fins) { if (f.chiffre_affaires) historique.push({ date: f.annee + '-12-31', type: 'financier', label: 'CA ' + f.annee + ': ' + Math.round(f.chiffre_affaires / 1000000) + 'M€', ca: f.chiffre_affaires, resultat: f.resultat }); }
    historique.sort((a: any, b: any) => (a.date || '').localeCompare(b.date || ''));
    return {
      siren: ed.siren, denomination: ed.denomination || ed.nom_entreprise,
      forme_juridique: ed.forme_juridique, date_creation: ed.date_creation,
      effectif: ed.effectif, effectif_min: ed.effectif_min, effectif_max: ed.effectif_max,
      capital: ed.capital_formate, code_naf: ed.code_naf, libelle_naf: ed.libelle_code_naf,
      objet_social: (ed.objet_social || '').slice(0, 500),
      siege: ed.siege ? (ed.siege.adresse_ligne_1 + ', ' + ed.siege.code_postal + ' ' + ed.siege.ville) : null,
      actionnariat: ed.actionnariat || null,
      dirigeants: keyDirs.map((d: any) => ({ qualite: d.qualite, prenom: d.prenom, nom: d.nom, depuis: d.date_prise_de_poste })),
      dirigeants_complet: allDirs.map((d: any) => ({ qualite: d.qualite, prenom: d.prenom, nom: d.nom, depuis: d.date_prise_de_poste })),
      beneficiaires: bens.map((b: any) => ({ prenom: b.prenom, nom: b.nom, parts: b.pourcentage_parts, votes: b.pourcentage_votes })),
      finances: fins.map((f: any) => ({ annee: f.annee, ca: f.chiffre_affaires, ca_export: f.chiffre_affaires_export, resultat: f.resultat, effectif: f.effectif, croissance: f.taux_croissance_chiffre_affaires, marge_ebitda: f.taux_marge_EBITDA, marge_operationnelle: f.taux_marge_operationnelle, delai_clients_jours: f.delai_paiement_clients_jours, delai_fournisseurs_jours: f.delai_paiement_fournisseurs_jours })),
      historique
    };
  } catch { return null; }
}

async function callClaude(sys: string, usr: string, max = 4000, timeoutMs = 70000): Promise<string> {
  _claudeDiag = null;
  if (!ANTHROPIC_KEY) { _claudeDiag = {raw_len:0,stop:'no_key',in_tok:0,out_tok:0}; return ''; }
  const ctrl = new AbortController(); const timer = setTimeout(() => ctrl.abort(), timeoutMs);
  const t0 = Date.now();
  try {
    console.log('Claude STREAM call: model=' + CLAUDE_MODEL + ' max=' + max + ' timeout=' + timeoutMs + 'ms');
    const r = await fetch('https://api.anthropic.com/v1/messages', {
      method: 'POST', signal: ctrl.signal,
      headers: { 'x-api-key': ANTHROPIC_KEY, 'anthropic-version': '2023-06-01', 'Content-Type': 'application/json' },
      body: JSON.stringify({ model: CLAUDE_MODEL, max_tokens: max, stream: true, system: sys, messages: [{ role: 'user', content: usr }] })
    });
    if (!r.ok) { clearTimeout(timer); const errBody = await r.text(); const el = ((Date.now() - t0) / 1000).toFixed(1); console.log('Claude HTTP ' + r.status + ' in ' + el + 's body=' + errBody.slice(0, 300)); _claudeDiag = {raw_len:0,stop:'http_'+r.status,in_tok:0,out_tok:0,err:errBody.slice(0,100)}; return ''; }
    // Parse SSE stream
    const reader = r.body!.getReader();
    const decoder = new TextDecoder();
    let text = '', buffer = '', stop_reason = '', input_tokens = 0, output_tokens = 0;
    try {
      while (true) {
        const {done, value} = await reader.read();
        if (done) break;
        buffer += decoder.decode(value, {stream: true});
        const lines = buffer.split('\n');
        buffer = lines.pop()!;
        for (const line of lines) {
          if (!line.startsWith('data: ')) continue;
          const data = line.slice(6);
          if (data === '[DONE]') continue;
          try {
            const ev = JSON.parse(data);
            if (ev.type === 'content_block_delta' && ev.delta?.text) text += ev.delta.text;
            else if (ev.type === 'message_start' && ev.message?.usage) input_tokens = ev.message.usage.input_tokens || 0;
            else if (ev.type === 'message_delta') { stop_reason = ev.delta?.stop_reason || ''; output_tokens = ev.usage?.output_tokens || 0; }
          } catch { }
        }
      }
    } catch (streamErr: any) {
      // Stream aborted by timeout — but we may have accumulated partial text!
      console.log('Claude stream interrupted after ' + ((Date.now() - t0) / 1000).toFixed(1) + 's text=' + text.length + 'ch err=' + streamErr.message);
      if (text.length > 0) {
        _claudeDiag = {raw_len:text.length,stop:'stream_aborted',in_tok:input_tokens,out_tok:output_tokens,preview:text.slice(0,300),err:'aborted@' + text.length + 'ch'};
        clearTimeout(timer);
        return text; // Return partial text — repairJson can salvage it
      }
    }
    clearTimeout(timer);
    const elapsed = ((Date.now() - t0) / 1000).toFixed(1);
    _claudeDiag = {raw_len:text.length,stop:stop_reason||'unknown',in_tok:input_tokens,out_tok:output_tokens,preview:text.slice(0,300)};
    console.log('Claude STREAM OK in ' + elapsed + 's stop=' + stop_reason + ' in=' + input_tokens + ' out=' + output_tokens + ' text=' + text.length + 'ch');
    if (stop_reason === 'max_tokens') console.log('Claude WARNING: truncated by max_tokens!');
    return text;
  } catch (e: any) { clearTimeout(timer); const el = ((Date.now() - t0) / 1000).toFixed(1); console.log('Claude err after ' + el + 's: ' + e.message); _claudeDiag = {raw_len:0,stop:'error',in_tok:0,out_tok:0,err:e.message?.slice(0,80)}; return ''; }
}

function parseJson(raw: string): any {
  if (!raw) return null; try { return JSON.parse(raw); } catch { }
  try { const c = raw.replace(/```json\s*/g, '').replace(/```\s*/g, '').trim(); const m = c.match(/\{[\s\S]*\}/); if (m) return JSON.parse(m[0]); } catch { }
  return null;
}

function repairJson(raw: string): any {
  // Strip code fences first
  let s = raw.replace(/```json\s*/gi, '').replace(/```\s*/gi, '').trim();
  const start = s.indexOf('{');
  if (start < 0) return null;
  s = s.slice(start);

  // Iterative repair: try to close structures, if parse fails trim to last safe point and retry
  for (let round = 0; round < 8; round++) {
    try {
      // Track state with a STACK for correct closing order ({, [, {, [ → ], }, ], })
      let inStr = false, esc = false;
      const stack: string[] = [];
      for (const c of s) {
        if (esc) { esc = false; continue; }
        if (c === '\\' && inStr) { esc = true; continue; }
        if (c === '"') { inStr = !inStr; continue; }
        if (!inStr) {
          if (c === '{') stack.push('{');
          else if (c === '}') { if (stack.length > 0) stack.pop(); }
          else if (c === '[') stack.push('[');
          else if (c === ']') { if (stack.length > 0) stack.pop(); }
        }
      }
      let r = s;
      if (inStr) r += '"'; // close truncated string
      // Close open structures in correct reverse order
      while (stack.length > 0) {
        const opener = stack.pop();
        r += opener === '{' ? '}' : ']';
      }
      // Clean up invalid trailing patterns created by truncation
      r = r.replace(/,\s*\]/g, ']').replace(/,\s*\}/g, '}'); // trailing comma before closer
      r = r.replace(/"[^"]*"\s*:\s*\}/g, '}').replace(/"[^"]*"\s*:\s*\]/g, ']'); // key without value before closer
      r = r.replace(/,\s*\]/g, ']').replace(/,\s*\}/g, '}'); // 2nd pass after key removal
      return JSON.parse(r);
    } catch {
      // Parse failed — trim to last safe cut point (after last closing " or } or ])
      let inStr2 = false, esc2 = false, lastSafe = 0;
      for (let i = 0; i < s.length; i++) {
        const c = s[i];
        if (esc2) { esc2 = false; continue; }
        if (c === '\\' && inStr2) { esc2 = true; continue; }
        if (c === '"') {
          if (inStr2) { inStr2 = false; lastSafe = i + 1; }
          else inStr2 = true;
          continue;
        }
        if (!inStr2 && (c === '}' || c === ']')) lastSafe = i + 1;
      }
      if (lastSafe > 1 && lastSafe < s.length) {
        s = s.slice(0, lastSafe).replace(/,\s*$/, '');
        console.log('repairJson round ' + round + ': trimmed to ' + s.length + 'ch');
      } else {
        break; // Can't trim further
      }
    }
  }
  return null;
}

async function loadCtx() {
  const [kg, ac, lb] = await Promise.all([
    sb('kg_context?select=section,content&is_active=eq.true'),
    sb('agent_config?select=*'),
    sb('livres_blancs?select=titre,resume,personas_cibles,segments_pertinents,landing_page')]);
  const m: Record<string, string> = {}; (kg || []).forEach((x: any) => { m[x.section] = x.content; }); return { kg: m, ac: ac || [], lb: lb || [] };
}

function computeWarmth(posts: any[], tg: any, news: any[]): { score: number, signals: string[], frequency: string, lastActive: string | null } {
  const signals: string[] = []; let score = 0; const now = Date.now();
  let frequency = 'inactive'; let lastActive = null;

  if (!posts || posts.length === 0) {
    signals.push('Aucun post prospect détecté');
  } else {
    const dates = posts.map((p: any) => new Date(p.date || p.posted_at || p.timestamp || 0).getTime()).filter(d => d > 0).sort((a, b) => b - a);
    lastActive = dates[0] ? new Date(dates[0]).toISOString() : null;
    const recentPosts = dates.filter(d => now - d < 30 * 86400000).length;

    if (recentPosts >= 8) { frequency = 'daily'; score += 3; signals.push('Poste quasi quotidiennement'); }
    else if (recentPosts >= 3) { frequency = 'weekly'; score += 2; signals.push('Poste régulièrement'); }
    else if (recentPosts >= 1) { frequency = 'monthly'; score += 1; signals.push('Poste occasionnellement'); }
    else { frequency = 'rare'; signals.push('Peu actif sur LinkedIn'); }

    const avgLikes = posts.reduce((s: number, p: any) => s + (p.likes_count || p.likes || p.num_likes || 0), 0) / posts.length;
    if (avgLikes > 50) { score += 2; signals.push('Fort engagement (' + Math.round(avgLikes) + ' likes/post)'); }
    else if (avgLikes > 10) { score += 1; signals.push('Engagement moyen (' + Math.round(avgLikes) + ' likes/post)'); }
    if (dates[0] && now - dates[0] < 7 * 86400000) { score += 2; signals.push('Post dans les 7 derniers jours'); }
    else if (dates[0] && now - dates[0] < 30 * 86400000) { score += 1; signals.push('Post dans les 30 derniers jours'); }

    const allText = posts.map((p: any) => (p.text || p.content || p.title || '')).join(' ').toLowerCase();
    if (allText.match(/recrut|hiring|talent|équipe commerciale/)) { score += 1; signals.push('En recrutement commercial'); }
    if (allText.match(/croissan|scale|hyper-growth|levée|fundrais/)) { score += 1; signals.push('Parle de croissance'); }
  }

  // Alternative signals for warmth if LinkedIn is quiet or missing
  if (news && news.length > 0) {
    score += 2; signals.push('Actualités ou PR récentes');
  }
  if (tg && tg.signal_faible && tg.signal_faible.length > 5) {
    score += 1; signals.push('Signal faible identifié (' + tg.signal_faible + ')');
  }

  if (score === 0) { score = 2; signals.push('Warmth par défaut (faible)'); }

  return { score: Math.min(score, 10), signals, frequency, lastActive };
}

async function getFeedback(cid: string): Promise<string> {
  const strats = await sb('communication_strategies?select=message_connexion,message_connexion_edited,message_suivi_1,message_suivi_edited,edit_count&order=created_at.desc&limit=10' + (cid ? '&consultant_id=eq.' + encodeURIComponent(cid) : ''));
  if (!strats || strats.length === 0) return ''; const patterns: string[] = []; let tE = 0, tS = 0;
  for (const s of strats) {
    tS++; if (s.edit_count > 0) tE++;
    if (s.message_connexion_edited && s.message_connexion) {
      if (s.message_connexion_edited.length < s.message_connexion.length * 0.7) patterns.push('Raccourcit les messages');
      if (s.message_connexion_edited.length > s.message_connexion.length * 1.3) patterns.push('Allonge les messages');
    }
  }
  const eR = tS > 0 ? Math.round(tE / tS * 100) : 0; const u = [...new Set(patterns)];
  if (u.length === 0 && eR < 20) return ''; return 'FEEDBACK ÉDITIONS (taux:' + eR + '%):' + u.join('. ') + '. Adapte ton style.';
}

function autoDURR(tg: any, liC: any, news: any[], pappers: any): { d: boolean, u: boolean, r1: boolean, r2: boolean, score: number, notes: string } {
  let d = false, u = false, r1 = false, r2 = false; const notes: string[] = [];
  if (pappers?.finances?.length >= 2) {
    const f0 = pappers.finances[0], f1 = pappers.finances[1];
    if (f0.ca && f1.ca && f0.ca <= f1.ca) { d = true; notes.push('CA stagnation/baisse:' + Math.round(f0.ca / 1e6) + 'M vs ' + Math.round(f1.ca / 1e6) + 'M'); }
    if (f0.resultat && f0.resultat < 0) { d = true; notes.push('Résultat négatif:' + Math.round(f0.resultat / 1000) + 'K€'); }
  }
  if (tg.signal_faible && (tg.signal_faible.includes('CA') || tg.signal_faible.includes('performance'))) { d = true; notes.push('Signal:' + tg.signal_faible); }
  if (tg.signal_faible && tg.signal_faible.match(/LBO|levée|acquisition|restructur|nomination|nouveau/i)) { u = true; notes.push('Urgence:' + tg.signal_faible); }
  const newsText = (news || []).map(n => n.titre || '').join(' ').toLowerCase();
  if (newsText.match(/levée|acquisition|rachat|lbo|restructur|nomination|nommé|nommée|nouveau.*directeur|nouvelle.*directrice|rejoint|appointed|head of/)) { u = true; notes.push('News urgentes'); }
  if (tg.statut_prospection && tg.statut_prospection !== 'nouveau' && tg.statut_prospection !== 'non_contacte') { r1 = true; notes.push('Prospect en cours'); }
  if (pappers?.finances?.length >= 3) {
    const tr = pappers.finances.map((f: any) => f.croissance).filter((c: any) => c != null);
    if (tr.length >= 2 && tr.every((c: any) => c < 5)) { r2 = true; notes.push('Croissance faible pluriannuelle'); }
  }
  return { d, u, r1, r2, score: (d ? 1 : 0) + (u ? 1 : 0) + (r1 ? 1 : 0) + (r2 ? 1 : 0), notes: notes.join(' | ') };
}

function extractOrgInfo(liC: any, webTeam: string | null, pappers: any): { employees: number | null, salesTeamSize: number | null, salesDetail: string | null, dircoName: string | null, dircoTitle: string | null } {
  let employees: number | null = null, dircoName: string | null = null, dircoTitle: string | null = null;
  let salesRoles: any[] = [], salesTeamSize: number | null = null;
  if (liC) {
    const co = Array.isArray(liC) ? liC[0] : liC;
    // v45: BD Company Profile returns employees_count as number or company_size as string like "201-500 employees"
    employees = co.employees_count || co.num_employees || co.staff_count || null;
    if (!employees && co.company_size) {
      const sizeStr = String(co.company_size);
      // Parse range formats: "201-500", "201-500 employees", "10,001+"
      const rangeMatch = sizeStr.match(/(\d[\d,]*)\s*[-–]\s*(\d[\d,]*)/);
      if (rangeMatch) { employees = Math.round((parseInt(rangeMatch[1].replace(/,/g, '')) + parseInt(rangeMatch[2].replace(/,/g, ''))) / 2); }
      else { const numMatch = sizeStr.match(/(\d[\d,]*)/); if (numMatch) employees = parseInt(numMatch[1].replace(/,/g, '')) || null; }
    }
    if (typeof employees === 'string') employees = parseInt(employees) || null;
    const people = co.employees || co.people || co.staff || [];
    if (Array.isArray(people)) {
      const dPat = /directeur commercial|directrice commerciale|vp sales|vp of sales|vice.?president.*sales|head of sales|chief revenue|chief commercial|sales director|directeur des ventes|cro\b/i;
      const sPat = /commercial|sales|business develop|account.?manager|account.?executive|\bsdr\b|\bbdr\b|inside sales|key account|charg[ée].*affaires|ingénieur commercial|responsable.*compte|vente/i;
      for (const p of people) {
        const t = (p.title || p.job_title || p.position || '');
        if (t.toLowerCase().match(dPat) && !dircoName) { dircoName = ((p.first_name || p.name || '') + ' ' + (p.last_name || '')).trim(); dircoTitle = t; }
        if (t.toLowerCase().match(sPat)) salesRoles.push({ name: ((p.first_name || p.name || '') + ' ' + (p.last_name || '')).trim(), title: t });
      }
      salesTeamSize = salesRoles.length > 0 ? salesRoles.length : null;
    }
  }
  if (!dircoName && webTeam) {
    const pats = [/(?:directeur|directrice)\s+commercial(?:e)?\s*[-–:]?\s*([A-ZÀ-Ü][a-zà-ü]+\s+[A-ZÀ-Ü][a-zà-ü]+)/gi,
      /(?:VP|Head of)\s+Sales\s*[-–:]?\s*([A-ZÀ-Ü][a-zà-ü]+\s+[A-ZÀ-Ü][a-zà-ü]+)/gi,
      /(?:Chief Revenue Officer|CRO)\s*[-–:]?\s*([A-ZÀ-Ü][a-zà-ü]+\s+[A-ZÀ-Ü][a-zà-ü]+)/gi];
    for (const pat of pats) { const m = pat.exec(webTeam); if (m) { dircoName = m[1].trim(); dircoTitle = '(via site web)'; break; } }
  }
  if (!dircoName && pappers?.dirigeants_complet) {
    for (const d of pappers.dirigeants_complet) {
      if ((d.qualite || '').toLowerCase().match(/commercial|vente|développement/)) { dircoName = ((d.prenom || '') + ' ' + (d.nom || '')).trim(); dircoTitle = d.qualite; break; }
    }
  }
  const salesDetail = salesRoles.length > 0 ? salesRoles.map(r => r.name + ' (' + r.title + ')').join(' | ') : null;
  return { employees, salesTeamSize, salesDetail, dircoName, dircoTitle };
}

function cors(r: Response): Response { r.headers.set('Access-Control-Allow-Origin', '*'); r.headers.set('Access-Control-Allow-Headers', '*'); return r; }

Deno.serve(async (req: Request) => {
  if (req.method === 'OPTIONS') return new Response(null, { headers: { 'Access-Control-Allow-Origin': '*', 'Access-Control-Allow-Headers': '*', 'Access-Control-Allow-Methods': 'POST, OPTIONS' } });
  try {
    const body = await req.json(); const { target_id, consultant_name, mode } = body;
    if (!target_id) return cors(new Response(JSON.stringify({ error: 'target_id requis' }), { status: 400, headers: { 'Content-Type': 'application/json' } }));
    const tgs = await sb('targets?id=eq.' + target_id + '&select=*'); const tg = tgs?.[0];
    if (!tg) return cors(new Response(JSON.stringify({ error: 'Cible introuvable' }), { status: 404, headers: { 'Content-Type': 'application/json' } }));
    if (mode === 'refresh_news') {
      const news = await fetchApifyGoogleSearch(tg.entreprise); const up: any = { last_news_check_at: new Date().toISOString() };
      if (news.length > 0) up.dernieres_news = news; await sb('targets?id=eq.' + target_id, { method: 'PATCH', body: JSON.stringify(up) });
      return cors(new Response(JSON.stringify({ success: true, mode: 'refresh_news', news_count: news.length }), { status: 200, headers: { 'Content-Type': 'application/json' } }));
    }
    await sb('targets?id=eq.' + target_id, { method: 'PATCH', body: JSON.stringify({ statut_prospection: 'en_cours' }) });
    const ctx = await loadCtx(); const con = consultant_name || tg.consultant_name || 'David Zaoui';
    const ac = ctx.ac.find((c: any) => c.consultant_name === con) || ctx.ac[0];
    const liUrl = tg.linkedin_ceo || tg.linkedin_contact;
    // DirCo URL: use linkedin_dirco, or linkedin_contact if different from CEO
    const dircoUrl = tg.linkedin_dirco || (tg.linkedin_contact && tg.linkedin_contact !== liUrl ? tg.linkedin_contact : null);
    const T0 = Date.now();
    console.log('v9.0: ' + tg.entreprise + ' liUrl=' + liUrl + ' dircoUrl=' + dircoUrl + ' site=' + tg.site_web + ' liEnt=' + tg.linkedin_entreprise);
    // Parallel data sources + feedback + early Pappers if SIREN known
    // Posts are extracted from BD Person profile (.activity field) — NOT from BD Posts dataset
    // Also fetch DirCo profile if URL available (for their activity too)
    const earlyPappers = tg.siren ? fetchPappers(tg.entreprise, tg.siren, null) : null;
    const [sc, feedback] = await Promise.all([
      Promise.allSettled([
        liUrl ? bdDataset(DS_LI_PERSON, [{ url: liUrl }], 40000) : Promise.resolve(null),                 // 0: CEO person (includes .activity = posts)
        tg.linkedin_entreprise ? bdDataset(DS_LI_COMPANY, [{ url: tg.linkedin_entreprise }], 40000) : Promise.resolve(null), // 1: company (includes .updates = posts)
        tg.site_web ? fetchApifyWebScrape(tg.site_web) : Promise.resolve({ main: null, about: null, team: null, legal: null, legalInfo: null }), // 2: website
        fetchApifyGoogleSearch(tg.entreprise),                                                              // 3: news
        earlyPappers || Promise.resolve(null),                                                              // 4: pappers
        dircoUrl ? bdDataset(DS_LI_PERSON, [{ url: dircoUrl }], 40000) : Promise.resolve(null),            // 5: DirCo person (includes .activity = posts)
      ]),
      getFeedback(ac?.consultant_id || '')
    ]);
    console.log('v9.0 data fetch done in ' + ((Date.now() - T0) / 1000).toFixed(1) + 's');
    const gv = (r: PromiseSettledResult<any>) => r.status === 'fulfilled' ? r.value : null;
    const gvErr = (r: PromiseSettledResult<any>) => r.status === 'rejected' ? String(r.reason).slice(0, 100) : null;
    // Log each source result
    console.log('v9.0 allSettled: liPerson=' + sc[0].status + (gvErr(sc[0]) || '') + ' liCompany=' + sc[1].status + (gvErr(sc[1]) || '') + ' web=' + sc[2].status + (gvErr(sc[2]) || '') + ' news=' + sc[3].status + (gvErr(sc[3]) || '') + ' pappers=' + sc[4].status + (gvErr(sc[4]) || '') + ' liDirco=' + sc[5].status + (gvErr(sc[5]) || ''));
    const liP = gv(sc[0]), liC = gv(sc[1]), webData = gv(sc[2]) || { main: null, about: null, team: null, legal: null, legalInfo: null };
    const news = gv(sc[3]);
    const liDirco = gv(sc[5]); // DirCo profile data

    // === EXTRACT POSTS from BD Person profiles (.activity) and Company profile (.updates) ===
    function extractPersonActivity(profileData: any, label: string): any[] {
      const profile = Array.isArray(profileData) ? profileData[0] : profileData;
      if (!profile) { console.log('v9.0 posts ' + label + ': profile data is NULL'); return []; }
      const activity = profile.activity || profile.posts || [];
      if (!Array.isArray(activity) || activity.length === 0) {
        console.log('v9.0 posts ' + label + ': no activity in profile (keys=' + Object.keys(profile).join(',') + ')');
        return [];
      }
      const posts = activity.map((a: any) => ({
        _source: label,
        text: a.text || a.title || a.content || '',
        link: a.link || a.url || a.post_url || '',
        post_url: a.post_url || a.link || a.url || '',
        interaction: a.interaction || '',
        date: a.date || a.posted_at || a.time || '',
        likes_count: a.likes_count || 0,
        id: a.id || ''
      })).filter((p: any) => p.text && p.text.length > 10);
      console.log('v9.0 posts ' + label + ': ' + posts.length + ' posts extracted from profile activity (raw=' + activity.length + ')');
      return posts;
    }
    function extractCompanyPosts(companyData: any): any[] {
      const company = Array.isArray(companyData) ? companyData[0] : companyData;
      if (!company) return [];
      const updates = company.updates || company.posts || company.activity || company.recent_updates || [];
      if (!Array.isArray(updates) || updates.length === 0) {
        console.log('v9.0 posts Company: no updates field (keys=' + Object.keys(company).join(',') + ')');
        return [];
      }
      // Company updates: text=post content, title=company name (not useful), post_url=direct link
      const posts = updates.map((u: any) => ({
        _source: 'Company',
        text: u.text || u.content || u.headline || '',
        link: u.post_url || u.link || u.url || '',
        post_url: u.post_url || u.link || u.url || '',
        date: u.date || u.posted_at || '',
        likes_count: u.likes_count || 0,
        comments_count: u.comments_count || 0,
        id: u.post_id || ''
      })).filter((p: any) => p.text && p.text.length > 20);
      console.log('v9.0 posts Company: ' + posts.length + ' posts extracted from company updates (raw=' + updates.length + ')');
      return posts;
    }
    const ceoPosts = extractPersonActivity(liP, 'CEO');
    const dircoPosts = extractPersonActivity(liDirco, 'DirCo');
    const companyPosts = extractCompanyPosts(liC);
    let prospectPostsMerged = [...ceoPosts, ...dircoPosts, ...companyPosts];
    console.log('v9.0 POSTS TOTAL: CEO=' + ceoPosts.length + ' DirCo=' + dircoPosts.length + ' Company=' + companyPosts.length + ' merged=' + prospectPostsMerged.length);
    if (news && (Array.isArray(news) ? news.length > 0 : true)) {
      const na = Array.isArray(news) ? news : [news];
      console.log('v9.0 news raw: count=' + na.length + ' first=' + (na[0] ? JSON.stringify(na[0]).slice(0, 200) : 'undefined'));
    } else { console.log('v9.0 news raw: ' + (news === null ? 'NULL' : 'empty[]')); }
    let pappers = gv(sc[4]);

    // Pappers: if early call failed or wasn't attempted, try with SIREN from web scrape
    const webSiren = webData.legalInfo?.siren;
    const webDenom = webData.legalInfo?.denomination;
    if (!pappers) {
      const pappSiren = webSiren || tg.siren || null;
      console.log('v9.0 Pappers: webSiren=' + webSiren + ' webDenom=' + webDenom + ' dbSiren=' + tg.siren);
      pappers = await fetchPappers(tg.entreprise, pappSiren, webDenom);
    }
    if (pappers) console.log('v9.0 Pappers OK: ' + pappers.denomination + ' SIREN=' + pappers.siren + ' fins=' + pappers.finances?.length);
    else console.log('v9.0 Pappers: NOT FOUND for ' + tg.entreprise);

    const liPS = liP ? JSON.stringify(Array.isArray(liP) ? liP[0] : liP).slice(0, 2500) : null;
    const liDircoS = liDirco ? JSON.stringify(Array.isArray(liDirco) ? liDirco[0] : liDirco).slice(0, 2000) : null;
    const liCS = liC ? JSON.stringify(Array.isArray(liC) ? liC[0] : liC).slice(0, 2000) : null;
    const webS = [webData.main?.slice(0, 2000), webData.about ? 'ABOUT:' + webData.about.slice(0, 1500) : null].filter(Boolean).join('\n');
    // News: use fetched news, fallback to DB stored news
    let newsArr = Array.isArray(news) && news.length > 0 ? news : null;
    if (!newsArr && tg.dernieres_news && Array.isArray(tg.dernieres_news) && tg.dernieres_news.length > 0) {
      newsArr = tg.dernieres_news;
      console.log('v9.0 News: using DB fallback (' + newsArr.length + ' articles from targets.dernieres_news)');
    }
    const newsS = newsArr ? JSON.stringify(newsArr).slice(0, 2000) : null;
    // Posts: balanced sampling from CEO + DirCo + Company (don't let one source monopolize)
    let postsArr: any[] = [];
    const maxPerSource = 5; // At most 5 from each source
    postsArr.push(...ceoPosts.slice(0, maxPerSource));
    postsArr.push(...dircoPosts.slice(0, maxPerSource));
    postsArr.push(...companyPosts.slice(0, maxPerSource));
    // Fill remaining slots from any source with leftovers (up to 15 total)
    const remaining = [...ceoPosts.slice(maxPerSource), ...dircoPosts.slice(maxPerSource), ...companyPosts.slice(maxPerSource)];
    postsArr.push(...remaining.slice(0, 15 - postsArr.length));
    console.log('v9.0 postsArr balanced: CEO=' + postsArr.filter((p: any) => p._source === 'CEO').length + ' DirCo=' + postsArr.filter((p: any) => p._source === 'DirCo').length + ' Co=' + postsArr.filter((p: any) => p._source === 'Company').length + ' total=' + postsArr.length);
    // Posts fallback: if all BD sources returned empty, try previous strategy's stored posts
    if (postsArr.length === 0) {
      const prevStrat = await sb('communication_strategies?target_id=eq.' + target_id + '&select=prospect_posts_data&order=created_at.desc&limit=1');
      if (prevStrat?.[0]?.prospect_posts_data && Array.isArray(prevStrat[0].prospect_posts_data) && prevStrat[0].prospect_posts_data.length > 0) {
        postsArr = prevStrat[0].prospect_posts_data;
        console.log('v9.0 Posts: using DB fallback (' + postsArr.length + ' posts from previous strategy)');
      }
    }
    const postsS = postsArr.length > 0 ? JSON.stringify(postsArr).slice(0, 4000) : null;
    const pappersS = pappers ? JSON.stringify(pappers).slice(0, 3000) : null;
    console.log('v9.0 DATA SUMMARY: liP=' + (liPS ? liPS.length + 'ch' : 'NULL') + ' liDirco=' + (liDircoS ? liDircoS.length + 'ch' : 'NULL') + ' liC=' + (liCS ? liCS.length + 'ch' : 'NULL') + ' web=' + (webS ? webS.length + 'ch' : 'NULL') + ' news=' + (newsArr?.length || 0) + ' posts=' + postsArr.length + '(CEO=' + ceoPosts.length + '/DirCo=' + dircoPosts.length + '/Co=' + companyPosts.length + ') pappers=' + (pappersS ? 'OK' : 'NULL'));
    const warmth = computeWarmth(postsArr, tg, newsArr || []);
    const orgInfo = extractOrgInfo(liC, webData.team, pappers);
    const durr = autoDURR(tg, liC, newsArr || [], pappers);

    // === SINGLE UNIFIED TARGET UPDATE === (fix: merge all updates into one PATCH)
    const targetUpdate: any = {
      warmth_score: warmth.score, warmth_signals: warmth.signals,
      prospect_last_active_at: warmth.lastActive, prospect_post_frequency: warmth.frequency,
      last_news_check_at: new Date().toISOString(),
      intelligence_enriched_at: new Date().toISOString(), intelligence_source: 'generate-strategy-v9.0',
    };
    if (newsArr) targetUpdate.dernieres_news = newsArr;
    // Pappers enrichment
    if (pappers) {
      if (pappers.siren && !tg.siren) targetUpdate.siren = pappers.siren;
      if (pappers.finances?.[0]?.ca && !tg.ca) targetUpdate.ca = String(Math.round(pappers.finances[0].ca / 1e6)) + 'M EUR';
      if (pappers.finances?.[0]?.croissance && !tg.taux_croissance) targetUpdate.taux_croissance = pappers.finances[0].croissance + '%';
      if (pappers.finances?.[0]?.resultat) targetUpdate.ebitda = String(Math.round(pappers.finances[0].resultat / 1e6)) + 'M EUR';
      if (pappers.effectif_min && !tg.effectif) targetUpdate.effectif = pappers.effectif;
      if (pappers.dirigeants?.length > 0) {
        const pres = pappers.dirigeants.find((d: any) => d.qualite === 'Président');
        if (pres && !tg.ceo_nom) { targetUpdate.ceo_prenom = pres.prenom; targetUpdate.ceo_nom = pres.nom; }
      }
      if (pappers.historique?.length > 0) targetUpdate.historique_entreprise = pappers.historique;
      if (pappers.dirigeants_complet?.length > 0) targetUpdate.dirigeants_complet = pappers.dirigeants_complet;
      if (pappers.beneficiaires?.length > 0) targetUpdate.beneficiaires = pappers.beneficiaires;
      if (pappers.finances?.length > 0) targetUpdate.finances_historique = pappers.finances;
      if (pappers.forme_juridique) targetUpdate.forme_juridique = pappers.forme_juridique;
      if (pappers.objet_social) targetUpdate.objet_social = pappers.objet_social;
      if (pappers.actionnariat) targetUpdate.actionnariat = pappers.actionnariat;
      if (pappers.siege) targetUpdate.siege_social = pappers.siege;
      if (pappers.date_creation) targetUpdate.date_creation_entreprise = pappers.date_creation;
      if (pappers.capital) targetUpdate.capital_social = pappers.capital;
      if (pappers.code_naf) targetUpdate.naf_code = pappers.code_naf;
    } else if (webData.legalInfo) {
      // Even without full Pappers, store what we found from legal page
      if (webData.legalInfo.siren && !tg.siren) targetUpdate.siren = webData.legalInfo.siren;
      if (webData.legalInfo.forme_juridique && !tg.forme_juridique) targetUpdate.forme_juridique = webData.legalInfo.forme_juridique;
    }
    // DirCo/sales team
    if (orgInfo.employees && !tg.effectif) targetUpdate.effectif = String(orgInfo.employees);
    if (orgInfo.salesTeamSize) targetUpdate.taille_equipe_commerciale = String(orgInfo.salesTeamSize);
    if (orgInfo.dircoName && !tg.dirco_nom) { targetUpdate.dirco_nom = orgInfo.dircoName; if (orgInfo.dircoTitle) targetUpdate.dirco_titre = orgInfo.dircoTitle; }
    // DURR auto (always update, overwrite) — NOTE: durr_score is a GENERATED column (auto-computed from d+u+r1+r2), do NOT include it in PATCH
    targetUpdate.durr_d = durr.d; targetUpdate.durr_u = durr.u; targetUpdate.durr_r1 = durr.r1; targetUpdate.durr_r2 = durr.r2;
    targetUpdate.durr_notes = durr.notes;

    // SINGLE PATCH to targets — v45: added detailed error logging
    console.log('v9.0 target update keys: ' + Object.keys(targetUpdate).join(', '));
    const tuBody = JSON.stringify(targetUpdate);
    console.log('v9.0 target update body size: ' + tuBody.length + 'ch');
    const tuResp = await fetch(SUPABASE_URL + '/rest/v1/targets?id=eq.' + target_id, { method: 'PATCH', headers: { ...SB_H, 'Prefer': 'return=representation' }, body: tuBody });
    if (!tuResp.ok) {
      const tuErr = await tuResp.text();
      console.error('v9.0 TARGET PATCH FAILED: HTTP ' + tuResp.status + ' body=' + tuErr.slice(0, 500));
    } else {
      const tuResult = await tuResp.json();
      console.log('v9.0 TARGET PATCH OK: ' + (tuResult?.length || 0) + ' rows updated, warmth=' + tuResult?.[0]?.warmth_score + ' durr=' + tuResult?.[0]?.durr_score);
    }

    const matchingLB = ctx.lb.filter((lb: any) => (lb.segments_pertinents || []).some((s: string) => s === tg.segment)).slice(0, 5);
    const bestPost = postsArr.length > 0 ? postsArr.sort((a: any, b: any) => {
      const da = new Date(a.date || a.posted_at || 0).getTime(), db = new Date(b.date || b.posted_at || 0).getTime(); return db - da;
    })[0] : null;

    // PROMPT V9.0 — Stratège d'échecs
    const kgC = Object.values(ctx.kg).join('\n').slice(0, 4000);
    const acC = ac ? ['NOM: ' + ac.consultant_name, 'TITRE: ' + (ac.titre || ''), 'TON DE VOIX: ' + (ac.ton_voix || ''),
    'PARCOURS COMPLET: ' + (ac.parcours_narratif || ''), 'ANECDOTES TERRAIN: ' + (ac.anecdotes_pro || ''),
    'SECTEURS D\'EXPERTISE: ' + (ac.secteurs_expertise || []).join(', '), 'TYPES CLIENTS: ' + (ac.types_clients || []).join(', '),
    'MOTS INTERDITS: ' + (ac.mots_interdits || []).join(', '), 'INSTRUCTIONS SPÉCIFIQUES: ' + (ac.system_prompt_extra || '')].join('\n') : 'Consultant: ' + con;
    const dm = ac?.longueur_dm || 85, cm = ac?.longueur_comment || 45, em = ac?.longueur_email || 200;
    const tgC = ['Entreprise:' + tg.entreprise + ' | Segment:' + (tg.segment || '?') + ' | Spécialité:' + (tg.specialite || '?'),
    'CA:' + (tg.ca || targetUpdate.ca || '?') + ' | Effectif:' + (tg.effectif || targetUpdate.effectif || '?') + ' | Croissance:' + (tg.taux_croissance || targetUpdate.taux_croissance || '?'),
    'CEO:' + (tg.ceo_prenom || '') + ' ' + (tg.ceo_nom || targetUpdate.ceo_nom || '?'),
    'DirCo:' + (orgInfo.dircoName || ((tg.dirco_prenom || '') + ' ' + (tg.dirco_nom || '')).trim() || 'NON') + (orgInfo.dircoTitle || tg.dirco_titre ? ' (' + (orgInfo.dircoTitle || tg.dirco_titre) + ')' : '') + (dircoUrl ? ' [LinkedIn: ' + dircoUrl + ']' : ''),
    'Équipe sales:' + (orgInfo.salesTeamSize ? orgInfo.salesTeamSize + ' pers.' : 'INCONNU (Veuillez utiliser les sources de données pour déduire le nombre de commerciaux ou taille d\'équipe)'),
    orgInfo.salesDetail ? 'Détail:' + orgInfo.salesDetail.slice(0, 300) : '',
    'Signal:' + (tg.signal_faible || '?'),
    'DURR auto: D=' + (durr.d ? 'OUI' : 'non') + ' U=' + (durr.u ? 'OUI' : 'non') + ' R=' + (durr.r1 ? 'OUI' : 'non') + ' R=' + (durr.r2 ? 'OUI' : 'non'),
    'Warmth:' + warmth.score + '/10 | Posts:' + warmth.frequency].join('\n');
    const dC = ['PROFIL_LINKEDIN_CEO:', liPS || 'Vide',
      liDircoS ? 'PROFIL_LINKEDIN_DIRCO:' + liDircoS : '',
      'ENTREPRISE_LINKEDIN:', liCS || 'Vide',
      'SITE_WEB:', webS?.slice(0, 2000) || 'Vide', webData.team ? 'PAGE_EQUIPE:' + webData.team.slice(0, 800) : '',
      'ACTUALITES:', newsS || 'Vide',
      'POSTS_LINKEDIN (CEO+DirCo+Entreprise):', postsS || 'Aucun',
      bestPost ? 'POST_A_COMMENTER:' + JSON.stringify(bestPost).slice(0, 500) : '',
      'PAPPERS:', pappersS || 'Vide',
      'LIVRES_BLANCS:' + matchingLB.map((lb: any) => lb.titre + (lb.resume ? ' (' + lb.resume.slice(0, 40) + ')' : '')).join(' | ')].join('\n');
    const sys = `[IDENTITÉ & PHILOSOPHIE]
Tu es un stratège de prospection B2B de haut niveau. Tu conçois des stratégies de communication UNIQUES pour chaque prospect, comme un grand maître d'échecs qui planifie 15 coups à l'avance.
Chaque action — commentaire LinkedIn, message de connexion, DM, email — est un coup calculé dans une séquence stratégique de 60 jours. Rien n'est aléatoire. Tout sert un objectif précis.

PHILOSOPHIE FONDAMENTALE :
• Tu ne vends JAMAIS les services de Keep Growing. Jamais. Aucun pitch, aucune présentation de services dans les messages.
• Ton objectif unique : créer une relation authentique entre le consultant et le prospect.
• Chaque interaction vise à COMPRENDRE les douleurs du prospect — pas à proposer une solution.
• L'écoute active est ton arme : questions ouvertes, partage de valeur, preuves concrètes que tu comprends son monde.
• Tu construis une histoire en 60 jours. Chaque message est un chapitre. Le prospect doit sentir une progression naturelle, pas une séquence marketing.

[CONTEXTE KEEP GROWING]
${kgC}

[CONSULTANT QUI EXÉCUTERA CETTE STRATÉGIE]
${acC}
${feedback || ''}

[MÉTHODE D'ANALYSE PRÉ-STRATÉGIE]
Avant de rédiger le moindre mot, tu ANALYSES en profondeur TOUTES les données fournies :
1. PROFIL DIRIGEANT (CEO) : parcours professionnel complet, ancienneté au poste, style de leadership visible dans ses posts, motivations, combats professionnels
2. PROFIL DIRCO (si identifié) : même profondeur + défis opérationnels typiques (montée en compétences équipe, process, recrutement, performance)
3. POSTS LINKEDIN DU PROSPECT : thèmes récurrents, ton utilisé, centres d'intérêt professionnels, problématiques évoquées entre les lignes, engagement de sa communauté
4. ENTREPRISE : proposition de valeur, positionnement marché, modèle commercial, concurrence perçue
5. DONNÉES FINANCIÈRES (Pappers) : CA et trajectoire sur 3 ans, résultat net, effectif, signaux de stress (baisse CA, résultat négatif) ou d'opportunité (croissance forte, levée de fonds)
6. ACTUALITÉS : recrutements commerciaux, levées de fonds, acquisitions, nominations, restructurations — tout signal de CHANGEMENT
7. DURR (ANALYSE INTERNE — ne JAMAIS utiliser ce jargon dans les messages) :
   - D (Douloureux) : le problème commercial fait-il mal financièrement ou opérationnellement ?
   - U (Urgent) : y a-t-il un facteur temps (LBO, nomination récente, levée, objectifs trimestriels) ?
   - R (Reconnu) : le dirigeant reconnaît-il publiquement le problème (posts, interviews) ?
   - R (Récurrent) : le problème dure-t-il depuis plusieurs trimestres/années (visible dans les finances) ?

Cette analyse profonde nourrit CHAQUE mot de ta stratégie. Si une donnée manque, DÉDUIS-LA intelligemment du contexte disponible.
ESTIMATION OBLIGATOIRE de la taille d'équipe commerciale : utilise le CA, l'effectif total, le secteur, le modèle commercial et les profils LinkedIn visibles pour estimer le nombre de commerciaux. Formule indicative : SaaS B2B → ~15-20% de l'effectif en commercial ; ESN/ICT → ~25-35% ; industrie → ~10-15%. Toujours donner un nombre ou une fourchette, JAMAIS "inconnu".

[PRINCIPES DE COMMUNICATION]
1. TON DE PAIR : chaque message sonne comme un message d'un confrère dirigeant qui partage un vécu. Pas un consultant qui prospecte. Le consultant parle depuis son expérience de CEO/dirigeant ayant géré des transformations, PAS depuis son rôle de consultant. Les commentaires LinkedIn en particulier doivent être indistinguables de ceux d'un pair entrepreneur — aucun biais commercial détectable.
2. PERSONNALISATION RADICALE : chaque message DOIT contenir au moins une référence concrète au prospect (un de ses posts, un chiffre Pappers, un fait de son parcours, une actualité).
3. COMMENTAIRES LINKEDIN MULTIPLES : génère 4 à 6 commentaires sur des posts DIFFÉRENTS du prospect (CEO + DirCo + page Entreprise). Chaque commentaire :
   - RÈGLE N°1 : le commentaire RÉPOND AU SUJET DU POST, pas au sujet de Keep Growing. Si le post parle de recrutement tech → commente sur le recrutement tech. Si le post parle d'internationalisation → commente sur l'internationalisation. Ne redirige JAMAIS vers des sujets de consulting commercial.
   - Apporte une VRAIE VALEUR au lecteur du post : un insight original, un retour d'expérience concret, un chiffre surprenant, une question qui fait réfléchir, un angle que personne n'a vu. Le commentaire doit enrichir la discussion, pas la détourner.
   - Ne mentionne JAMAIS Keep Growing, le consulting, la performance commerciale, la structuration sales, le diagnostic commercial, ni aucun service du consultant. Le commentaire ne doit avoir AUCUNE arrière-pensée commerciale visible.
   - Le consultant commente EN TANT QUE DIRIGEANT qui a vécu des situations similaires, pas en tant que consultant qui prospecte. Il partage son expérience de terrain sur LE SUJET DU POST.
   - Exemples de BONS commentaires : "Intéressant. On a testé cette approche avec 3 pays en parallèle — le piège c'est de vouloir dupliquer le playbook FR tel quel. Ce qui a marché : adapter le cycle de décision local avant le messaging." / "Question sincère : comment vous mesurez l'impact de ça sur le terrain ? J'ai souvent vu un gap entre l'annonce et l'exécution à 6 mois."
   - Exemples de MAUVAIS commentaires (À NE PAS FAIRE) : "La structuration commerciale est clé pour scaler..." / "Vos rituels commerciaux sont-ils calibrés pour..." / "Le passage à l'échelle commercial est le prochain défi..." → ces formulations trahissent une intention de prospection.
   - Crée une PRÉSENCE et une FAMILIARITÉ avant la demande de connexion
   - Est suffisamment intelligent pour que le prospect ait envie de regarder le profil du consultant
   - DOIT inclure le post_url (lien direct vers le post LinkedIn) et la source (CEO/DirCo/Company)
   - Les commentaires doivent être répartis sur des posts de sources VARIÉES (CEO, DirCo, page Entreprise) quand disponibles
4. PROGRESSION STRATÉGIQUE SUR 60 JOURS — RÈGLE FONDAMENTALE : COMMENTER PENDANT 1 MOIS AVANT TOUT DM :
   - Phase 1 (J0→J30) : 4-6 commentaires intelligents ÉTALÉS sur 30 jours → se faire remarquer, devenir un visage familier. Espacer : 1-2 commentaires/semaine max. Varier les posts (CEO, DirCo, page Entreprise).
   - Phase 2 (J30→J35) : Connexion personnalisée référençant les échanges en commentaires ("on a échangé plusieurs fois en commentaires...")
   - Phase 3 (J35→J45) : Premier DM de valeur — insight unique, question ouverte, partage d'expérience terrain
   - Phase 4 (J45→J55) : Nurturing — guide pertinent, cas client similaire, contenus de valeur sans pression
   - Phase 5 (J55→J60) : Proposition de RDV 15min avec valeur claire et spécifique, sans urgence artificielle
5. MOTS STRICTEMENT INTERDITS dans TOUS les messages : "je me permets", "notre solution", "optimiser", "booster", "n'hésitez pas", "je serais ravi", "bénéficier de", "ROI", "synergie", "win-win", "accompagnement", "partenariat", "structuration commerciale", "performance commerciale", "rituels commerciaux", "passage à l'échelle", "diagnostic commercial", "process sales"
   THÈMES INTERDITS DANS LES COMMENTAIRES LINKEDIN : ne JAMAIS orienter un commentaire vers la structuration sales, l'optimisation commerciale, le coaching commercial ou tout sujet qui trahit une intention de vente de services de consulting. Le commentaire doit être 100% sur le sujet du post.
6. SIGNATURE : prénom du consultant uniquement, jamais nom complet ni titre

[DIFFÉRENCIATION CEO vs DIRCO]
APPROCHE CEO :
• Vision stratégique, transformation, héritage professionnel, impact durable
• Parler d'égal à égal : tendances marché, enjeux sectoriels, comparaisons concurrence
• Accrocher en reliant son PARCOURS personnel à un enjeu de transformation commerciale

APPROCHE DIRCO :
• Opérationnel, performance équipe, quick wins mesurables, gestion du quotidien
• Parler en praticien : chiffres terrain, vécu opérationnel, défis concrets de management commercial
• Accrocher en reliant ses DÉFIS QUOTIDIENS à une problématique système (pas de personnes)

Si CEO + DirCo identifiés → STRATÉGIE DE TENAILLE OBLIGATOIRE :
• Tu DOIS générer la séquence COMPLÈTE pour le DirCo (commentaires, connexion, DM, email, relances, nurturing, RDV). AUCUN champ DirCo ne doit être vide.
• Deux angles complémentaires qui convergent vers le même diagnostic. Les messages CEO et DirCo ne doivent PAS se ressembler — angles, ton et références différents.
• DirCo : angle opérationnel (performance équipe, process, recrutement). CEO : angle stratégique (vision, transformation, marché).
• Les commentaires DirCo ciblent ses posts ET les posts de la page entreprise (angle opérationnel). Les commentaires CEO ciblent ses posts (angle stratégique).

[OBJECTIONS À ANTICIPER]
Pour chaque prospect, anticipe 3-4 objections SPÉCIFIQUES à sa situation (pas génériques). Prépare des réponses pour le consultant. Chaque réponse doit : (1) valider l'objection avec empathie, (2) poser une question ouverte qui provoque la réflexion, (3) s'appuyer sur un fait CONCRET du prospect (chiffre, post, actualité).

[FORMAT DE SORTIE]
IMPÉRATIF : Réponds UNIQUEMENT avec le JSON demandé. Aucun texte avant ou après. Aucun backtick.
CONCISION : Sois PERCUTANT et CONCIS dans chaque champ. Les messages doivent respecter strictement les limites de mots indiquées. L'ensemble du JSON doit être compact — privilégie l'impact à la longueur.`;

    const usr = 'CIBLE:\n' + tgC + '\nDONNÉES COLLECTÉES:\n' + dC +
      '\n\nGÉNÈRE LE JSON SUIVANT (STRICT, aucun texte autour, aucun backtick) :\n' +
      '{"analyse_prospect":{"synthese":["3 observations clés"],' +
      '"parcours_ceo":"parcours CEO en 2-3 phrases",' +
      '"parcours_dirco":"parcours DirCo bref ou Non identifié",' +
      '"proposition_valeur":"proposition de valeur en 1-2 phrases",' +
      '"enjeux_identifies":["3 enjeux business"],' +
      '"style_communication":"style LinkedIn en 1 phrase"},' +
      '"trigger_event":"événement déclencheur principal",' +
      '"angle_attaque":"angle stratégique principal en 1 phrase percutante",' +
      '"niveau_confiance":"high|medium|low",' +
      '"strategie_globale":"vision stratégie 60j en 3-4 phrases",' +
      '"warmth_assessment":"réceptivité en 1-2 phrases",' +
      '"commentaires_linkedin":[{"post_cible":"sujet du post","post_url":"lien LinkedIn exact du post","source":"CEO ou DirCo ou Company","commentaire":"<' + cm + ' mots","objectif":"rôle stratégique"},' +
      '{"post_cible":"2e post","post_url":"lien","source":"CEO/DirCo/Company","commentaire":"<' + cm + ' mots","objectif":"..."},' +
      '{"post_cible":"3e post","post_url":"lien","source":"...","commentaire":"<' + cm + ' mots","objectif":"..."},' +
      '{"post_cible":"4e post","post_url":"lien","source":"...","commentaire":"<' + cm + ' mots","objectif":"..."},' +
      '{"post_cible":"5e post","post_url":"lien","source":"...","commentaire":"<' + cm + ' mots","objectif":"..."},' +
      '{"post_cible":"6e post","post_url":"lien","source":"...","commentaire":"<' + cm + ' mots","objectif":"..."}],' +
      '"approche_ceo":"stratégie CEO en 2-3 phrases",' +
      '"message_connexion":"<280 chars connexion CEO",' +
      '"message_suivi_1":"<' + dm + ' mots DM post-connexion",' +
      '"message_email":"Objet:\\n\\nCorps <' + em + ' mots",' +
      '"message_relance_guide":"<60 mots partage guide",' +
      '"message_relance_temoignage":"<60 mots cas client",' +
      '"message_proposition_rdv":"<80 mots proposition RDV",' +
      '"message_nurturing_long":"<120 mots email nurturing",' +
      '"approche_dirco":"stratégie DirCo COMPLÈTE en 2-3 phrases (OBLIGATOIRE si DirCo identifié, vide UNIQUEMENT si aucun DirCo)",' +
      '"dirco_commentaires_linkedin":[{"post_cible":"post","post_url":"lien","source":"DirCo ou Company","commentaire":"<' + cm + ' mots","objectif":"..."},{"post_cible":"2e","post_url":"lien","source":"...","commentaire":"<' + cm + ' mots","objectif":"..."}],' +
      '"dirco_commentaire_linkedin":"meilleur commentaire DirCo (pour rétro-compat)",' +
      '"dirco_message_connexion":"<280 chars (OBLIGATOIRE si DirCo identifié)",' +
      '"dirco_message_suivi":"<' + dm + ' mots (OBLIGATOIRE si DirCo identifié)",' +
      '"dirco_message_email":"email DirCo (OBLIGATOIRE si DirCo identifié)",' +
      '"dirco_message_relance_guide":"<60 mots (OBLIGATOIRE si DirCo identifié)",' +
      '"dirco_message_relance_temoignage":"<60 mots (OBLIGATOIRE si DirCo identifié)",' +
      '"dirco_message_proposition_rdv":"<80 mots (OBLIGATOIRE si DirCo identifié)",' +
      '"dirco_message_nurturing":"<120 mots (OBLIGATOIRE si DirCo identifié)",' +
      '"objections_anticipees":[{"objection":"objection spécifique","reponse_consultant":"réponse + question ouverte"},' +
      '{"objection":"2e","reponse_consultant":"..."},{"objection":"3e","reponse_consultant":"..."}],' +
      '"timeline_60_jours":[{"periode":"J0-J10","actions_ceo":"2 commentaires CEO","actions_dirco":"1 commentaire DirCo/Company","objectif":"se faire remarquer"},' +
      '{"periode":"J10-J20","actions_ceo":"2 commentaires CEO","actions_dirco":"1 commentaire DirCo/Company","objectif":"devenir familier"},' +
      '{"periode":"J20-J30","actions_ceo":"connexion CEO","actions_dirco":"connexion DirCo","objectif":"capitaliser sur la familiarité des commentaires"},' +
      '{"periode":"J30-J45","actions_ceo":"DM + suivi CEO","actions_dirco":"DM + suivi DirCo","objectif":"créer la relation"},' +
      '{"periode":"J45-J60","actions_ceo":"nurturing + RDV CEO","actions_dirco":"nurturing + RDV DirCo","objectif":"convertir en rendez-vous"}],' +
      '"strategie_branches":{"si_connexion_acceptee":"action","si_connexion_refusee":"plan B","si_pas_de_reponse_dm":"relance","si_reponse_positive":"accélération","si_reponse_negative":"nurturing"},' +
      '"sujets_interet":["s1","s2","s3"],"hooks_identifies":["h1","h2","h3"],' +
      '"ressource_a_partager":"titre du guide pertinent",' +
      '"post_prospect_reference":{"content":"post clé","date":"date","url":"lien","why":"pertinence"},' +
      '"durr_assessment":{"douloureux":true,"douloureux_detail":"fait","urgent":true,"urgent_detail":"fait","reconnu":true,"reconnu_detail":"fait","recurrent":true,"recurrent_detail":"fait"},' +
      '"intelligence_update":{"industrie":"secteur","taille_equipe_commerciale":"NOMBRE ESTIMÉ OBLIGATOIRE (déduis du CA, effectif, secteur, LinkedIn — ex: SaaS B2B 20M€ CA ~280 pers = ~30-50 commerciaux)","dirco_identifie":"nom ou vide","clients_cles":["client1"],"go_to_market":"modèle commercial"}}';
    const t0c = Date.now();
    const elapsedBeforeClaude = ((Date.now() - T0) / 1000).toFixed(1);
    // Claude timeout: remaining wall clock minus 8s safety for DB writes, clamped [40s, 120s]
    // With streaming, even if we abort, repairJson can salvage the partial response
    const claudeTimeout = Math.min(Math.max(150000 - (Date.now() - T0) - 8000, 40000), 120000);
    console.log('v9.0 calling Claude: sys=' + sys.length + ' usr=' + usr.length + ' total=' + (sys.length + usr.length) + ' elapsed=' + elapsedBeforeClaude + 's timeout=' + (claudeTimeout / 1000).toFixed(0) + 's');
    const rawOrig = await callClaude(sys, usr, 16000, claudeTimeout);
    // Strip markdown code fences that Claude sometimes adds despite explicit instructions
    const raw = rawOrig.replace(/```json\s*/gi, '').replace(/```\s*/gi, '').trim();
    let p = parseJson(raw);
    console.log('v9.0 Claude done in ' + ((Date.now() - t0c) / 1000).toFixed(1) + 's rawOrig=' + (rawOrig?.length || 0) + ' cleaned=' + (raw?.length || 0) + ' parsed=' + !!p + ' diag=' + JSON.stringify(_claudeDiag || {}).slice(0, 200));
    // If raw exists but JSON parse failed, try to extract JSON
    if (!p && raw && raw.length > 100) {
      console.log('v9.0 parse fail — raw starts with: ' + JSON.stringify(raw.slice(0, 80)) + ' ends with: ' + JSON.stringify(raw.slice(-80)));
      // Try brace-match (string-aware: skip braces inside "..." strings)
      try {
        const start = raw.indexOf('{');
        if (start >= 0) {
          let depth = 0, end = -1, inStr = false, esc = false;
          for (let i = start; i < raw.length; i++) {
            const c = raw[i];
            if (esc) { esc = false; continue; }
            if (c === '\\' && inStr) { esc = true; continue; }
            if (c === '"') { inStr = !inStr; continue; }
            if (!inStr) {
              if (c === '{') depth++;
              else if (c === '}') { depth--; if (depth === 0) { end = i; break; } }
            }
          }
          if (end > start) { p = JSON.parse(raw.slice(start, end + 1)); console.log('v9.0 brace-match parse OK len=' + (end - start + 1)); }
          else console.log('v9.0 brace-match: no balanced close found (truncated) depth=' + depth + ' start=' + start);
        }
      } catch (bmErr: any) { console.log('v9.0 brace-match parse failed: ' + bmErr.message?.slice(0, 100)); }
      // If still not parsed, try repair (handles truncated JSON from stream abort / max_tokens)
      if (!p) {
        console.log('v9.0 attempting repairJson on ' + raw.length + 'ch...');
        p = repairJson(raw);
        if (p) console.log('v9.0 repairJson OK — salvaged truncated response! keys=' + Object.keys(p).join(','));
        else console.log('v9.0 repairJson FAILED — falling back to default strategy');
      }
    }
    if (!p && !raw) console.log('v9.0 CRITICAL: Claude empty — timeout or API error after ' + elapsedBeforeClaude + 's data fetch. Diag: ' + JSON.stringify(_claudeDiag));
    const usedFallback = !p;
    if (!p) p = { analyse_prospect: { synthese: ['Données insuffisantes — enrichir le profil'], parcours_ceo: '', parcours_dirco: '', proposition_valeur: '', enjeux_identifies: [], style_communication: '' }, trigger_event: tg.signal_faible || '?', angle_attaque: 'À définir', niveau_confiance: 'low', strategie_globale: 'Données insuffisantes pour ' + tg.entreprise + '. Enrichir le profil avant de lancer la stratégie.', commentaires_linkedin: [], message_connexion: '', message_suivi_1: '', message_email: '', message_relance_guide: '', message_relance_temoignage: '', message_proposition_rdv: '', message_nurturing_long: '', warmth_assessment: '', approche_ceo: '', approche_dirco: '', dirco_commentaire_linkedin: '', dirco_message_connexion: '', dirco_message_suivi: '', dirco_message_email: '', dirco_message_relance_guide: '', dirco_message_relance_temoignage: '', dirco_message_proposition_rdv: '', dirco_message_nurturing: '', objections_anticipees: [], timeline_60_jours: [], hooks_identifies: [], sujets_interet: [], ressource_a_partager: '', strategie_branches: {}, durr_assessment: {}, intelligence_update: {} };

    // DURR from AI — update targets with AI assessment (SINGLE additional PATCH)
    const da = p.durr_assessment || {};
    const aiDurrUpdate: any = {};
    if (da.douloureux !== undefined) {
      aiDurrUpdate.durr_d = !!da.douloureux || durr.d;
      aiDurrUpdate.durr_u = !!da.urgent || durr.u;
      aiDurrUpdate.durr_r1 = !!da.reconnu || durr.r1;
      aiDurrUpdate.durr_r2 = !!da.recurrent || durr.r2;
      // NOTE: durr_score is a GENERATED column — do NOT include in PATCH (auto-computed from d+u+r1+r2)
      aiDurrUpdate.durr_notes = [da.douloureux_detail, da.urgent_detail, da.reconnu_detail, da.recurrent_detail].filter(Boolean).join(' | ');
    }
    // Intelligence from AI
    const iu = p.intelligence_update || {};
    if (iu.industrie && !tg.industrie) aiDurrUpdate.industrie = iu.industrie;
    if (iu.taille_equipe_commerciale) {
      // Extract clean numeric value from AI response (e.g., "30-50" from "environ 30-50 commerciaux")
      let salesEst = String(iu.taille_equipe_commerciale);
      const rangeM = salesEst.match(/(\d+)\s*[-–àa]\s*(\d+)/);
      if (rangeM) salesEst = rangeM[1] + '-' + rangeM[2];
      else { const singleM = salesEst.match(/(\d+)/); if (singleM) salesEst = singleM[1]; }
      aiDurrUpdate.taille_equipe_commerciale = salesEst;
    }
    if (iu.dirco_identifie && !tg.dirco_nom) aiDurrUpdate.dirco_nom = iu.dirco_identifie;
    if (iu.clients_cles?.length > 0) aiDurrUpdate.clients_cles = iu.clients_cles;
    if (iu.go_to_market) aiDurrUpdate.go_to_market = iu.go_to_market;
    // Fix post_url: replace AI-hallucinated URLs with real scraped post URLs
    const usedPostIdx = new Set();
    function matchPostUrl(comments: any[], posts: any[]) {
      if (!comments?.length || !posts?.length) return;
      for (const c of comments) {
        const src = (c.source || '').toLowerCase();
        // Find best matching post by source + text similarity
        let bestIdx = -1, bestScore = -1;
        for (let i = 0; i < posts.length; i++) {
          if (usedPostIdx.has(i)) continue;
          const pSrc = (posts[i]._source || '').toLowerCase();
          const srcMatch = (src.includes('ceo') && pSrc === 'ceo') || (src.includes('dirco') && pSrc === 'dirco') || (src.includes('company') && pSrc === 'company');
          const pText = (posts[i].text || '').toLowerCase().substring(0, 200);
          const cText = (c.post_cible || '').toLowerCase();
          // Simple word overlap scoring
          const cWords = cText.split(/\s+/).filter((w: string) => w.length > 3);
          const wordScore = cWords.filter((w: string) => pText.includes(w)).length;
          const score = (srcMatch ? 10 : 0) + wordScore;
          if (score > bestScore) { bestScore = score; bestIdx = i; }
        }
        if (bestIdx >= 0) {
          const realUrl = posts[bestIdx].post_url || posts[bestIdx].link || posts[bestIdx].url;
          if (realUrl) { c.post_url = realUrl; usedPostIdx.add(bestIdx); }
        }
      }
    }
    matchPostUrl(p.commentaires_linkedin, postsArr);
    matchPostUrl(p.dirco_commentaires_linkedin, postsArr);
    console.log('v9.0 post_url fix: matched ' + usedPostIdx.size + ' real URLs from ' + postsArr.length + ' scraped posts');

    // Save strategy (DELETE then INSERT — sequential dependency)
    await fetch(SUPABASE_URL + '/rest/v1/communication_strategies?target_id=eq.' + target_id, { method: 'DELETE', headers: SB_H });
    const postRef = p.post_prospect_reference || null;
    if (postRef && !postRef.url && bestPost) { postRef.url = bestPost.post_url || bestPost.url || bestPost.link || (liUrl ? liUrl + '/recent-activity/' : null); }
    const sd = {
      target_id, consultant_id: ac?.consultant_id || null,
      // Analyse prospect (v9)
      analyse_prospect: p.analyse_prospect || null,
      profil_synthetise: p.analyse_prospect?.synthese || p.profil_synthetise || [],
      // Strategy core
      trigger_event: p.trigger_event || tg.signal_faible,
      angle_attaque: p.angle_attaque || '', niveau_confiance: p.niveau_confiance || 'low',
      strategie_globale: p.strategie_globale || '', warmth_assessment: p.warmth_assessment || '',
      // CEO sequence
      approche_ceo: p.approche_ceo || '',
      commentaire_linkedin: p.commentaires_linkedin?.[0]?.commentaire || p.commentaire_linkedin || '',
      commentaires_multi: p.commentaires_linkedin || null,
      message_connexion: p.message_connexion || '', message_suivi_1: p.message_suivi_1 || '',
      message_email: p.message_email || '',
      message_relance_guide: p.message_relance_guide || '',
      message_relance_temoignage: p.message_relance_temoignage || '',
      message_proposition_rdv: p.message_proposition_rdv || '',
      message_nurturing_long: p.message_nurturing_long || '',
      // DirCo full sequence (v9)
      approche_dirco: p.approche_dirco || '',
      dirco_commentaire_linkedin: p.dirco_commentaires_linkedin?.[0]?.commentaire || p.dirco_commentaire_linkedin || '',
      dirco_commentaires_multi: p.dirco_commentaires_linkedin || null,
      dirco_message_connexion: p.dirco_message_connexion || '', message_connexion_dirco: p.dirco_message_connexion || '',
      dirco_message_suivi: p.dirco_message_suivi || '', message_suivi_dirco: p.dirco_message_suivi || '',
      dirco_message_email: p.dirco_message_email || '',
      dirco_message_relance_guide: p.dirco_message_relance_guide || '',
      dirco_message_relance_temoignage: p.dirco_message_relance_temoignage || '',
      dirco_message_proposition_rdv: p.dirco_message_proposition_rdv || '',
      dirco_message_nurturing: p.dirco_message_nurturing || '',
      dirco_angle_attaque: (p.approche_dirco || '').split('.')[0] || '',
      // Objections & Timeline (v9)
      objections_anticipees: p.objections_anticipees || null,
      timeline_60_jours: p.timeline_60_jours || null,
      // Existing fields
      derniere_publication: postRef?.content || null,
      sujets_interet: p.sujets_interet || [], hooks_identifies: p.hooks_identifies || [],
      ressource_a_partager: p.ressource_a_partager || '', strategie_branches: p.strategie_branches || null,
      post_prospect_ref: postRef, prospect_posts_data: postsArr.length > 0 ? postsArr.slice(0, 5) : null,
      linkedin_person_data: liP ? (Array.isArray(liP) ? liP[0] : liP) : null,
      linkedin_company_data: liC ? (Array.isArray(liC) ? liC[0] : liC) : null,
      website_data: webS ? { content: webS, about: webData.about?.slice(0, 500) || null } : null,
      news_data: newsArr || null, pappers_data: pappers || null,
      data_sources_log: { linkedin_person: !!liPS, linkedin_company: !!liCS, website: !!webData.main, website_about: !!webData.about, website_team: !!webData.team, website_legal: !!webData.legal, news: newsArr?.length || 0, prospect_posts: postsArr.length, posts_ceo: postsArr.filter((p: any) => p._source === 'CEO').length, posts_dirco: postsArr.filter((p: any) => p._source === 'DirCo').length, posts_company: postsArr.filter((p: any) => p._source === 'Company').length, pappers: !!pappersS, claude: _claudeDiag ? { len: _claudeDiag.raw_len, stop: _claudeDiag.stop, in: _claudeDiag.in_tok, out: _claudeDiag.out_tok, parsed: !usedFallback, err: _claudeDiag.err || null, preview: _claudeDiag.preview?.slice(0, 200) || null } : null },
      // Steps tracking CEO
      step_commentaire_done: false, step_connexion_done: false, step_suivi_done: false,
      step_email_done: false, step_relance_done: false, step_rdv_done: false, step_nurturing_done: false,
      // Steps tracking DirCo
      dirco_step_commentaire_done: false, dirco_step_connexion_done: false, dirco_step_suivi_done: false,
      dirco_step_email_done: false, dirco_step_relance_done: false, dirco_step_rdv_done: false, dirco_step_nurturing_done: false,
      // Meta
      status: 'generated', ai_model: CLAUDE_MODEL, prompt_version: 'v9.0'
    };
    const ir = await fetch(SUPABASE_URL + '/rest/v1/communication_strategies', { method: 'POST', headers: { ...SB_H, 'Prefer': 'return=representation' }, body: JSON.stringify(sd) });
    if (!ir.ok) { const errB = await ir.text(); console.error('Strategy INSERT failed: ' + ir.status + ' ' + errB); return cors(new Response(JSON.stringify({ error: 'Strategy save failed: ' + ir.status }), { status: 500, headers: { 'Content-Type': 'application/json' } })); }
    const ins = await ir.json();
    // Merged final PATCH: AI DURR + status update (single call) + agent_config in parallel
    // v45: Also copy warmth_assessment from Claude to targets for display on Fiche tab
    const finalTargetUpdate: any = { ...aiDurrUpdate, statut_prospection: 'analyse_generee', pipeline_etape: 'strategie_generee', strategy_generated_at: new Date().toISOString() };
    if (p.warmth_assessment) finalTargetUpdate.warmth_assessment = p.warmth_assessment;
    const finalWrites: Promise<any>[] = [
      sb('targets?id=eq.' + target_id, { method: 'PATCH', body: JSON.stringify(finalTargetUpdate) })
    ];
    if (ac) finalWrites.push(sb('agent_config?consultant_name=eq.' + encodeURIComponent(con), { method: 'PATCH', body: JSON.stringify({ total_strategies_generated: (ac.total_strategies_generated || 0) + 1 }) }));
    await Promise.all(finalWrites);
    console.log('v9.0 TOTAL ' + tg.entreprise + ': ' + ((Date.now() - T0) / 1000).toFixed(1) + 's fallback=' + usedFallback);
    const finalDurrScore = [aiDurrUpdate.durr_d ?? durr.d, aiDurrUpdate.durr_u ?? durr.u, aiDurrUpdate.durr_r1 ?? durr.r1, aiDurrUpdate.durr_r2 ?? durr.r2].filter(Boolean).length;
    return cors(new Response(JSON.stringify({
      success: true, strategy_id: ins?.[0]?.id, target: tg.entreprise, consultant: con,
      confidence: p.niveau_confiance, trigger: p.trigger_event,
      warmth_score: warmth.score, durr_score: finalDurrScore,
      dirco_found: !!orgInfo.dircoName, sales_team_size: orgInfo.salesTeamSize,
      prompt_version: 'v9.0', used_fallback: usedFallback, has_branches: !!p.strategie_branches,
      has_60day_sequence: !!(p.timeline_60_jours?.length > 0),
      has_objections: !!(p.objections_anticipees?.length > 0),
      has_multi_comments: !!(p.commentaires_linkedin?.length > 1),
      pappers_found: !!pappers, pappers_siren: pappers?.siren || webSiren || null,
      legal_name: pappers?.denomination || webDenom || null,
      data_sources: { linkedin_person: !!liPS, linkedin_company: !!liCS, website: !!webData.main, website_about: !!webData.about, website_team: !!webData.team, website_legal: !!webData.legal, news: newsArr?.length || 0, prospect_posts: postsArr.length, pappers: !!pappersS }
    }), { status: 200, headers: { 'Content-Type': 'application/json' } }));
  } catch (e: any) {
    console.error('FATAL ' + e.message + ' ' + e.stack);
    return cors(new Response(JSON.stringify({ error: e.message }), { status: 500, headers: { 'Content-Type': 'application/json' } }));
  }
});
