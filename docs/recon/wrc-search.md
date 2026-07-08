# WRC Search — Recon Notes

Captured live via `scrapy shell` (2026). This is the contract the spider
relies on; re-verify with `scrapy shell` if the crawl starts returning zero
results.

## The form is ASP.NET, but it redirects to a GET endpoint

The advanced-search page (`/en/search/?advance=true`) is ASP.NET WebForms
(`__VIEWSTATE`, `ctl00$ContentPlaceHolder_Main$...` field names). Submitting it
**302-redirects to a plain GET URL**, which we query directly instead of
replaying the VIEWSTATE postback:

```
GET /en/search/?decisions=1&from=DD/MM/YYYY&to=DD/MM/YYYY&body=<code>&pageNumber=<n>
```

| Param | Meaning |
|-------|---------|
| `decisions=1` | search the Decisions & Determinations database |
| `from` / `to` | start / finish dates, `DD/MM/YYYY` (literal slashes) |
| `body` | numeric body code (below) |
| `pageNumber` | 1-based page; **10 results per page** |

## Body codes (from the checkbox `value` attributes)

The Body filter is the `CB2` checkbox group; each item's `value` attribute is
the `body=` code (NOT a 1–4 index — WRC is 15376):

| Body | code |
|------|------|
| Equality Tribunal | 1 |
| Employment Appeals Tribunal | 2 |
| Labour Court | 3 |
| Workplace Relations Commission | 15376 |

Equality Tribunal and the EAT were merged into the WRC c.2015, so they return
little/nothing for recent date ranges — that is correct, not a bug. These codes
live in `WRC_BODY_CODES` (config), not in code.

The `CB1` checkbox group is a document-type filter (Appeal / Complaint /
Enforcement / Industrial Relations Referral) — left unset.

## Result count

`Shows 1 to 10 of <N> results` → parsed by `RESULT_COUNT_RE`. The spider
computes `ceil(N / 10)` pages and fans out `pageNumber=2..pages`.

## Result card structure

Each result is a `<li class="each-item">`:

```html
<li class="each-item clearfix">
  <h2 class="title"><a href="/en/cases/2024/february/lcr22912.html">LCR22912</a></h2>
  <span class="date">30/01/2024</span>
  <p class="description" title="SONOMA VALLEY AND A WORKER">...</p>
  <div class="row bottom-ref">
    <span class="refNO">LCR22912</span>
    <a class="btn" href="/en/cases/2024/february/lcr22912.html">View Page</a>
  </div>
</li>
```

| Field | Selector |
|-------|----------|
| identifier | `span.refNO::text` (e.g. `LCR22912`, `ADJ-00047352`) |
| title | `h2.title a::text` |
| published_date | `span.date::text` (`DD/MM/YYYY`) |
| description | `p.description::attr(title)` (parties; collapse embedded newlines) |
| doc_url | `div.bottom-ref a::attr(href)` (falls back to `h2.title a`) |

## Documents

View-Page links go to case pages at `/en/cases/YYYY/month/<ref>.html` — the
decision is rendered as HTML on that page, so it's stored as `.html` (the
transform later strips boilerplate). Any link ending in `.pdf/.doc/.docx/.rtf`
is stored byte-for-byte instead.


## Official search guide

The advanced-search page links to the WRC Decisions Information Guide:

`https://www.workplacerelations.ie/en/publications_forms/decisions_information_guide.pdf`

The guide confirms:

- four searchable bodies;
- searches use the decision/determination date;
- decision records may be stored as HTML or PDF;
- PDF keyword searches examine only pre-tagged metadata rather than the
  document body;
- WRC Adjudication Officer decisions are available from October 2015 onward.

The guide itself is supporting documentation and is intentionally excluded
from ingestion because it is not a decision search-result record.