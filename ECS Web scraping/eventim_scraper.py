import asyncio, csv, os, io
from datetime import datetime
import boto3
from playwright.async_api import async_playwright

HOME_URL   = "https://www.eventim.com.br"
OUTPUT_CSV = "eventim_events.csv"  # não usado no ECS

async def collect_category_links(page):
    await page.goto(HOME_URL, wait_until="domcontentloaded"); await page.wait_for_timeout(2000)
    return await page.evaluate("""
        Array.from(
            document.querySelectorAll('ul[aria-label="Todos os eventos"] div.js-nav-sub-trigger[data-href]')
        ).map(div => window.location.origin + div.getAttribute('data-href'))
         .filter((v,i,a) => a.indexOf(v) === i)
    """)

async def scrape_category(page, category_url):
    await page.goto(category_url, wait_until="domcontentloaded"); await page.wait_for_timeout(2000)
    page_urls = await page.evaluate("""
        (() => {
            const hrefs = Array.from(document.querySelectorAll('listing-pagination a[href*="?p="]'))
                                .map(a => a.href);
            return Array.from(new Set(hrefs));
        })()
    """)
    if category_url not in page_urls: page_urls.insert(0, category_url)

    events, seen = [], set()
    for pu in page_urls:
        print("→ página:", pu)
        await page.goto(pu, wait_until="domcontentloaded"); await page.wait_for_timeout(2000)
        batch = await page.evaluate("""
            () => {
                const out = [];
                document.querySelectorAll('product-group-item').forEach(group => {
                    const artist = group
                        .querySelector('div[data-qa="product-group-headline"] span')
                        ?.innerText.trim() || null;
                    group.querySelectorAll('listing-cta a[href*="/event/"]').forEach(a => {
                        const url = a.href;
                        const card = a.closest('div.listing-item-clickable');
                        const sub1 = card.querySelector('div.listing-subheadline:first-of-type span');
                        let location = null, date = null;
                        if (sub1) {
                            const parts = sub1.innerText.split(',');
                            location = parts.shift().trim(); date = parts.join(',').trim();
                        }
                        out.push({ artist, url, location, date });
                    });
                });
                return out;
            }
        """)
        for ev in batch:
            if ev['url'] and ev['url'] not in seen:
                seen.add(ev['url']); events.append(ev)
    return events

async def main():
    async with async_playwright() as pw:
        browser = await pw.firefox.launch(headless=True)
        page    = await browser.new_page()
        await page.route("**/*", lambda r, req:
            r.abort() if req.resource_type in ("image","stylesheet","font") else r.continue_()
        )
        cats = await collect_category_links(page)
        print(f"→ {len(cats)} categorias encontradas")
        all_events = []
        for cat in cats:
            evs = await scrape_category(page, cat)
            print(f"  • {len(evs)} shows")
            all_events.extend(evs)
        await browser.close()

    # ==== Upload direto ao S3 ====
    bucket = os.getenv("S3_BUCKET")
    prefix = os.getenv("EVENTIM_PREFIX", "eventim/")
    if not bucket:
        raise RuntimeError("Variável de ambiente S3_BUCKET não definida")
    run_ts = datetime.utcnow().strftime("%Y%m%d-%H%M%S")
    key = f"{prefix}date={datetime.utcnow().strftime('%Y-%m-%d')}/eventim_{run_ts}.csv"

    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=["artist","url","location","date"])
    writer.writeheader(); writer.writerows(all_events)
    s3 = boto3.client("s3")
    s3.put_object(Bucket=bucket, Key=key, Body=buf.getvalue().encode("utf-8"), ContentType="text/csv")
    print(f"✓ Enviado para s3://{bucket}/{key} (total {len(all_events)} registros)")

if __name__ == "__main__":
    asyncio.run(main())
