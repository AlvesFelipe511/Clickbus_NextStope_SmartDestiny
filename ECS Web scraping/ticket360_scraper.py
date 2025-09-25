import asyncio, csv, os, io
from datetime import datetime
import boto3
from playwright.async_api import async_playwright

HOME_URL   = "https://www.ticket360.com.br/"
OUTPUT_CSV = "ticket360_all_events.csv"  # não usado no ECS
USER_AGENT = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
              "AppleWebKit/537.36 (KHTML, like Gecko) "
              "Chrome/114.0.0.0 Safari/537.36")

async def collect_routes(page):
    await page.goto(HOME_URL, wait_until="networkidle"); await page.wait_for_timeout(2000)
    return await page.evaluate("""
        () => Array.from(document.querySelectorAll('a[href]'))
                  .map(a => a.getAttribute('href').split('?')[0])
                  .filter(h => h.startsWith('/categoria/') || h.startsWith('/local/'))
                  .map(h => window.location.origin + h)
                  .filter((v,i,a) => a.indexOf(v) === i)
    """)

async def scrape_category(page, base_url):
    await page.goto(base_url, wait_until="networkidle"); await page.wait_for_timeout(2000)
    await page.evaluate("window.scrollTo(0, document.body.scrollHeight)"); await page.wait_for_timeout(1000)
    page_urls = await page.evaluate("""
        () => {
            const path = window.location.pathname;
            const urls = Array.from(document.querySelectorAll('a[href*="?p="]'))
                .map(a => a.href).filter(h => h.includes(path));
            return Array.from(new Set(urls)).sort((a,b) => {
                const pa = +new URL(a).searchParams.get('p') || 0;
                const pb = +new URL(b).searchParams.get('p') || 0;
                return pa - pb;
            });
        }
    """)
    if base_url not in page_urls: page_urls.insert(0, base_url)

    seen_urls, events = set(), []
    for pu in page_urls:
        print(f"→ Navegando em: {pu}")
        await page.goto(pu, wait_until="networkidle"); await page.wait_for_timeout(2000)
        await page.evaluate("window.scrollTo(0, document.body.scrollHeight)"); await page.wait_for_timeout(1000)
        raw = await page.evaluate("""
            () => {
                const anchors = Array.from(document.querySelectorAll('a.event-click'));
                const cards = anchors.filter(a => (a.getAttribute('href')||'').includes('evento/'));
                return cards.map(a => {
                    let href = a.getAttribute('href') || '';
                    if (!href.startsWith('http')) {
                        href = window.location.origin + '/' + href.replace(/^\\/+/, '');
                    }
                    const title = (a.getAttribute('title') || a.innerText).trim();
                    let place = null;
                    const hdr = a.querySelector('.header-card-event');
                    if (hdr) {
                        const elPlace = hdr.querySelector('.card-name-local strong');
                        place = elPlace ? elPlace.innerText.trim() : hdr.innerText.trim();
                    }
                    let city = null;
                    const cityEl = a.querySelector('.card-endereco');
                    if (cityEl) city = cityEl.innerText.trim();

                    let date = null;
                    const cal = a.querySelector('.row.data-calendar, .data-calendar');
                    if (cal) {
                        const month = cal.querySelector('.data-mes')?.innerText.trim() || '';
                        const day   = cal.querySelector('.data-layer')?.innerText.trim() || '';
                        const week  = cal.querySelector('.data-semana')?.innerText.trim() || '';
                        date = day && month ? `${day} ${month}${week? ' • ' + week : ''}` : null;
                    }
                    return { url: href, title, place, city, date };
                });
            }
        """)
        for ev in raw:
            if ev['url'] not in seen_urls:
                seen_urls.add(ev['url']); events.append(ev)
    return events

async def main():
    all_events = []
    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=True)
        ctx     = await browser.new_context(user_agent=USER_AGENT)
        page    = await ctx.new_page()
        routes  = await collect_routes(page)
        print(f"{len(routes)} rotas encontradas")
        for route in routes:
            evs = await scrape_category(page, route)
            all_events.extend(evs)
        await browser.close()

    # ==== Upload direto ao S3 ====
    bucket = os.getenv("S3_BUCKET")
    prefix = os.getenv("T360_PREFIX", "ticket360/")
    if not bucket:
        raise RuntimeError("Variável de ambiente S3_BUCKET não definida")
    run_ts = datetime.utcnow().strftime("%Y%m%d-%H%M%S")
    key = f"{prefix}date={datetime.utcnow().strftime('%Y-%m-%d')}/ticket360_{run_ts}.csv"

    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=["url","title","place","city","date"])
    writer.writeheader(); writer.writerows(all_events)
    s3 = boto3.client("s3")
    s3.put_object(Bucket=bucket, Key=key, Body=buf.getvalue().encode("utf-8"), ContentType="text/csv")
    print(f"✓ Enviado para s3://{bucket}/{key} (total {len(all_events)} registros)")

if __name__ == "__main__":
    asyncio.run(main())
