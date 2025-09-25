import asyncio
import os
import io
import csv
import re
from datetime import datetime
import boto3
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright

BASE_URL = "https://www.sympla.com.br"
BASE_LIST_URL = f"{BASE_URL}/eventos/show-musica-festa/todos-eventos"
DATE_RE = re.compile(r"\d{1,2}\s+de\s+\w+\s+às\s+\d{1,2}:\d{2}", re.IGNORECASE)


async def scrape_all():
    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=True)
        page = await browser.new_page()

        eventos, page_num = [], 1
        while True:
            url = f"{BASE_LIST_URL}?page={page_num}"
            print(f"Carregando página {page_num}: {url}")
            await page.goto(url, wait_until="domcontentloaded", timeout=60000)
            cards = BeautifulSoup(await page.content(), "html.parser").select("a.sympla-card")
            if not cards:
                print("Nenhum card encontrado — fim da listagem.")
                break

            for c in cards:
                href = c["href"]
                if href.startswith("/"):
                    href = BASE_URL + href
                title = c.get(
                    "data-name") or c.select_one("h3").get_text(strip=True)
                place = c.select_one("p").get_text(strip=True)

                date_text = None
                for div in c.find_all("div"):
                    t = div.get_text(strip=True)
                    m = DATE_RE.search(t)
                    if m:
                        date_text = m.group(0)
                        break

                eventos.append({"url": href, "title": title,
                               "place": place, "date": date_text})
            page_num += 1
        await browser.close()
    return eventos


def upload_to_s3(events):
    cols = ["url", "title", "place", "date"]
    bucket = os.getenv("S3_BUCKET")
    prefix = os.getenv("SYMPLA_PREFIX", "sympla/")
    if not bucket:
        raise RuntimeError("Variável de ambiente S3_BUCKET não definida")

    run_ts = datetime.utcnow().strftime("%Y%m%d-%H%M%S")
    key = f"{prefix}date={datetime.utcnow().strftime('%Y-%m-%d')}/sympla_{run_ts}.csv"

    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=cols)
    writer.writeheader()
    writer.writerows(events)

    s3 = boto3.client("s3")
    s3.put_object(Bucket=bucket, Key=key, Body=buf.getvalue().encode(
        "utf-8"), ContentType="text/csv")
    print(
        f"✓ Enviado para s3://{bucket}/{key} (total {len(events)} registros)")


if __name__ == "__main__":
    all_events = asyncio.run(scrape_all())
    upload_to_s3(all_events)
