import "jsr:@supabase/functions-js/edge-runtime.d.ts";

const SUPABASE_URL = Deno.env.get('SUPABASE_URL')!;
const SUPABASE_KEY = Deno.env.get('SUPABASE_SERVICE_ROLE_KEY')!;
const SB_H = { 'apikey': SUPABASE_KEY, 'Authorization': 'Bearer ' + SUPABASE_KEY, 'Content-Type': 'application/json' };
const CORS_H = { 'Access-Control-Allow-Origin': '*', 'Access-Control-Allow-Headers': 'authorization, x-client-info, apikey, content-type', 'Content-Type': 'application/json' };

// ─── Supabase helper ───
async function sb(path: string, opts: any = {}) {
  const r = await fetch(SUPABASE_URL + '/rest/v1/' + path, { headers: SB_H, ...opts });
  if (!r.ok) { console.log('SB err ' + r.status + ' ' + path.split('?')[0]); return null; }
  const t = await r.text();
  return t ? JSON.parse(t) : null;
}

// ─── Google News RSS — zero API key, fiable, gratuit ───
async function fetchGoogleNewsRSS(query: string): Promise<any[]> {
  try {
    const url = `https://news.google.com/rss/search?q=${encodeURIComponent(query)}&hl=fr&gl=FR&ceid=FR:fr`;
    console.log('RSS query: ' + query.substring(0, 60));
    const res = await fetch(url, {
      headers: { 'User-Agent': 'Mozilla/5.0 (compatible; PulseOutreach/1.0)' }
    });
    if (!res.ok) { console.log('RSS error: ' + res.status); return []; }
    const xml = await res.text();

    // Parse RSS XML items
    const items: any[] = [];
    const itemRegex = /<item>([\s\S]*?)<\/item>/g;
    let match;
    while ((match = itemRegex.exec(xml)) !== null && items.length < 8) {
      const itemXml = match[1];
      const title = (itemXml.match(/<title>([\s\S]*?)<\/title>/) || [])[1] || '';
      const link = (itemXml.match(/<link>([\s\S]*?)<\/link>/) || [])[1] || '';
      const pubDate = (itemXml.match(/<pubDate>([\s\S]*?)<\/pubDate>/) || [])[1] || '';
      const source = (itemXml.match(/<source[^>]*>([\s\S]*?)<\/source>/) || [])[1] || '';

      // Clean CDATA and HTML entities
      const cleanTitle = title
        .replace(/<!\[CDATA\[|\]\]>/g, '')
        .replace(/&amp;/g, '&')
        .replace(/&lt;/g, '<')
        .replace(/&gt;/g, '>')
        .replace(/&quot;/g, '"')
        .replace(/&#39;/g, "'")
        .trim();

      if (cleanTitle) {
        items.push({
          titre: cleanTitle,
          date: pubDate,
          source: link.replace(/<!\[CDATA\[|\]\]>/g, '').trim(),
          media: source.replace(/<!\[CDATA\[|\]\]>/g, '').trim()
        });
      }
    }
    console.log('RSS found: ' + items.length + ' items');
    return items;
  } catch (e) {
    console.log('RSS exception: ' + e);
    return [];
  }
}

// ─── Action suggestion generator ───
function generateSuggestion(title: string, contactName: string, company: string): string {
  const t = (title || '').toLowerCase();
  const who = contactName || company;
  if (t.match(/levée|fundrais|investis|série|fonds|million|financement/))
    return `Féliciter ${who} : "J'ai vu l'annonce de votre levée, bravo ! Ça doit accélérer vos ambitions commerciales..."`;
  if (t.match(/nommé|nommée|nomination|appointed|rejoint|arrivée|nouveau.*directeur|nouveau.*ceo/))
    return `Féliciter pour la nomination : "J'ai vu votre prise de poste, les 100 premiers jours sont clés pour structurer..."`;
  if (t.match(/recrut|hiring|embauche|recrutement|offre.*emploi/))
    return `Engager sur le recrutement : "Vous structurez l'équipe commerciale, c'est souvent le signal d'une belle accélération..."`;
  if (t.match(/acquisition|rachat|merge|fusion|racheté|rachète/))
    return `Rebondir sur l'acquisition : "L'intégration d'équipes commerciales post-acquisition est un moment charnière..."`;
  if (t.match(/partenariat|partnership|alliance|collaboration|s'associe/))
    return `Commenter le partenariat : "Un nouveau partenariat, c'est souvent un game-changer côté go-to-market..."`;
  if (t.match(/croissance|growth|chiffre.*affaires|résultat|performance|rentab/))
    return `Engager sur la croissance : "Votre trajectoire est impressionnante, le passage à l'échelle commercial est le prochain défi..."`;
  if (t.match(/innovation|ia |intelligence artificielle|tech|produit|lancement|lance/))
    return `Rebondir sur l'innovation : "J'ai vu votre dernière avancée, ça change la donne pour vos clients..."`;
  if (t.match(/événement|salon|conférence|webinar|event|mwc|vivatech/))
    return `Engager autour de l'événement : "J'ai vu que vous étiez présent à cet événement, les retours terrain sont toujours précieux..."`;
  return `Engager la conversation avec ${who} : "J'ai lu votre actualité récente, ça m'a interpellé..."`;
}

// ─── Safe parse dernieres_news (can be string or array) ───
function parseDernieresNews(raw: any): any[] {
  if (!raw) return [];
  if (Array.isArray(raw)) return raw;
  if (typeof raw === 'string') {
    try { const parsed = JSON.parse(raw); return Array.isArray(parsed) ? parsed : []; }
    catch { return []; }
  }
  return [];
}

// ─── Main handler ───
Deno.serve(async (req: Request) => {
  if (req.method === 'OPTIONS') return new Response('ok', { headers: CORS_H });

  try {
    const body = await req.json().catch(() => ({}));
    const filterConsultant = body.consultant_name || null;

    // 1. Get all targets with a strategy
    const strats = await sb('communication_strategies?select=target_id,created_at');
    if (!strats || !strats.length) {
      return new Response(JSON.stringify({ success: true, refreshed: 0, alerts: 0, message: 'No strategies found' }), { headers: CORS_H });
    }
    const stratTargetIds = new Set(strats.map((s: any) => s.target_id));

    // 2. Get targets
    let targetQuery = 'targets?select=id,entreprise,ceo_prenom,ceo_nom,consultant_name,last_news_check_at,dernieres_news';
    if (filterConsultant) targetQuery += '&consultant_name=eq.' + encodeURIComponent(filterConsultant);
    const allTargets = await sb(targetQuery);
    if (!allTargets) {
      return new Response(JSON.stringify({ success: false, error: 'Failed to fetch targets' }), { headers: CORS_H });
    }

    // 3. Filter: only targets with strategy + stale (not refreshed in 7 days)
    const now = Date.now();
    const SEVEN_DAYS = 7 * 24 * 60 * 60 * 1000;
    const staleTargets = allTargets.filter((t: any) => {
      if (!stratTargetIds.has(t.id)) return false;
      if (!t.last_news_check_at) return true;
      return (now - new Date(t.last_news_check_at).getTime()) > SEVEN_DAYS;
    });

    console.log(`Refresh: ${staleTargets.length} stale targets out of ${allTargets.length} total (${stratTargetIds.size} with strategy)`);

    // 4. Process each stale target
    let refreshed = 0, alerts = 0;
    const details: any[] = [];

    for (const tg of staleTargets) {
      try {
        const ceoName = [tg.ceo_prenom, tg.ceo_nom].filter(Boolean).join(' ');
        // Search query: company name + CEO name if available
        const searchQuery = ceoName && ceoName !== 'À identifier'
          ? `"${tg.entreprise}" OR "${ceoName}"`
          : `"${tg.entreprise}"`;

        const news = await fetchGoogleNewsRSS(searchQuery);

        // Update targets.dernieres_news + last_news_check_at
        const update: any = { last_news_check_at: new Date().toISOString() };
        if (news.length > 0) update.dernieres_news = news;
        await sb('targets?id=eq.' + tg.id, {
          method: 'PATCH',
          headers: { ...SB_H, 'Prefer': 'return=minimal' },
          body: JSON.stringify(update)
        });

        // Detect NEW news (compare with previously stored) — safe parse
        const oldNews = parseDernieresNews(tg.dernieres_news);
        const oldTitles = new Set(oldNews.map((n: any) => (n.titre || '').toLowerCase().trim()));
        const newNews = news.filter((n: any) => !oldTitles.has((n.titre || '').toLowerCase().trim()));

        // Insert new items into target_news
        for (const n of newNews) {
          const suggestion = generateSuggestion(n.titre, ceoName, tg.entreprise);
          await sb('target_news', {
            method: 'POST',
            headers: { ...SB_H, 'Prefer': 'return=minimal' },
            body: JSON.stringify({
              target_id: tg.id,
              consultant_name: tg.consultant_name,
              type: 'news',
              title: n.titre || 'Actualité',
              url: n.source || null,
              source: n.media || 'Google News',
              snippet: (n.titre || '').substring(0, 200),
              contact_name: ceoName || tg.entreprise,
              action_suggestion: suggestion,
              is_read: false
            })
          });
        }

        refreshed++;
        if (newNews.length > 0) alerts++;
        details.push({ entreprise: tg.entreprise, total_news: news.length, new_news: newNews.length });
        console.log(`✓ ${tg.entreprise}: ${news.length} news, ${newNews.length} new`);
      } catch (e) {
        console.log(`✗ ${tg.entreprise}: ${e}`);
        details.push({ entreprise: tg.entreprise, error: String(e) });
      }
    }

    return new Response(JSON.stringify({
      success: true,
      refreshed,
      alerts,
      total_with_strategy: stratTargetIds.size,
      stale_processed: staleTargets.length,
      details
    }), { headers: CORS_H });

  } catch (e) {
    console.log('Fatal error: ' + e);
    return new Response(JSON.stringify({ success: false, error: String(e) }), { status: 500, headers: CORS_H });
  }
});
