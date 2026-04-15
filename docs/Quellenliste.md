# Quellenliste — Open Mics Zürich

Übersicht der Datenquellen für Comedy- und Open-Mic-Events in und um Zürich.  
Einträge nach Priorität oder Startreihenfolge; später ergänzen.

**Wichtig:** Diese Datei ist die **menschliche Doku** (Ideen, Prioritäten, Hinweise).  
Die **technisch aktivierten** Quellen stehen in `scrapers/sources.py` (inkl. `id`, Start-URL, Extractor).  
Wenn du hier eine neue Quelle ergänzt, musst du sie danach in `scrapers/sources.py` eintragen (und ggf. einen neuen Extractor implementieren).

---

## Vorlage (neue Quelle)

| Feld | Inhalt |
|------|--------|
| Name | |
| Typ | Web / API / Feed / manuell |
| Start-URL oder Endpunkt | |
| Abdeckung | z. B. Stadt, Radius, Sprache |
| Nutzen fürs Projekt | Open Mic, Showcase, wiederkehrende Serien |
| Hinweise | Scraping, Rate-Limits, AGB |

---

## 1. Eventfrog (CH)

| Feld | Inhalt |
|------|--------|
| **Name** | Eventfrog |
| **Typ** | Web (Suchergebnisse, Ticket-/Eventseiten) |
| **Start-URL** | [Comedy & Cabaret Zürich — Suche «open mic», Radius 10 km](https://eventfrog.ch/en/events/zuerich/comedy-cabaret.html?searchTerm=open+mic&geoRadius=10) (entspricht der Standard-URL im Scraper) |
| **Abdeckung** | Zürich + Umland (`geoRadius=10`); Kategorie `comedy-cabaret` + `searchTerm` (z. B. `open+mic`) wie in der Eventfrog-UI. |
| **Nutzen** | Viele Comedy- und Open-Mic-Reihen (inkl. Event-Gruppen mit mehreren Terminen), Venue, Datum/Uhrzeit, oft Ticketpreis sichtbar. |
| **Hinweise** | Inhalte können per JavaScript nachgeladen werden — Scraper ggf. mit Playwright o. Ä.; [AGB/Datenschutz](https://eventfrog.ch) beachten; freundliche Abrufintervalle. |

**Varianten der gleichen Basis:**

- Sprache: Pfad `/de/events/...` oder `/fr/events/...` statt `/en/events/...` falls nötig.
- Suche verfeinern: `searchTerm` z. B. `open+mic` oder Kombinationen testen, Radius bei Bedarf ändern.

---

## 2. Zürich Tourismus — Event Finder

| Feld | Inhalt |
|------|--------|
| **Name** | Zürich Tourismus — Veranstaltungen in Zürich (“Event finden”) |
| **Typ** | Web (Event Finder UI) + eingebettete Guidle-Microsite (API) |
| **Start-URL** | https://www.zuerich.com/de/events-nachtleben/event-finden |
| **Abdeckung** | Breites Event-Verzeichnis; je nach Suche sehr viele Events |
| **Nutzen** | Alternative Quelle neben Eventfrog; kann andere Veranstalter/Venues enthalten |
| **Hinweise** | Technisch wird eine **Guidle Microsite** eingebettet und per API befüllt. Für Scraping besser die Guidle-API nutzen (stabiler als DOM-Scraping). Begriffe wie “open mic” können auch Musik/Jam-Sessions meinen → Filter nötig. |

**Im Scraper konfiguriert als:** `zuerich_com_event_finder` (siehe `scrapers/sources.py`).

---

## 3. Stubä Comedy — Milanski Comedy

| Feld | Inhalt |
|------|--------|
| **Name** | Stubä Comedy (Milanski Comedy) |
| **Typ** | Web (Serien-/Info-Seite) |
| **Start-URL** | https://www.milanski-comedy.ch/stubae-comedy |
| **Abdeckung** | Einzelne Serie (dienstags) |
| **Nutzen** | Deckt ein grosses deutschsprachiges Comedy Open Mic ab (Stubä). |
| **Hinweise** | Instagram ist oft unzuverlässig zu scrapen; bevorzugt die Website als kanonische Quelle. |

**Im Scraper konfiguriert als:** `stubae_comedy` (siehe `scrapers/sources.py`).

---

## Geplant (Platzhalter)

| Name | Notiz |
|------|--------|
| *(noch keine weiteren Quellen)* | Nach Eventfrog: z. B. Veranstalter-Websites, ComedyHaus-Kalender, Meetup, Facebook-Events. |
