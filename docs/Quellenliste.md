# Quellenliste — Open Mics Zürich

Übersicht der Datenquellen für Comedy- und Open-Mic-Events in und um Zürich.  
Einträge nach Priorität oder Startreihenfolge; später ergänzen.

**Wichtig:** Diese Datei ist die **Single Source of Truth** für die technisch aktivierten Quellen.  
Der Scraper liest die Quellen-Definitionen direkt aus dieser Datei (siehe `scrapers/sources.py`).

### Maschinenlesbarer Block (vom Scraper gelesen)

Für jede aktive Quelle muss es einen Block der Form geben:

```source
id: eventfrog_de
label: Eventfrog (DE, Zürich Comedy & Kabarett / Open Mic)
start_url: https://eventfrog.ch/de/events/zuerich/comedy-cabaret.html?searchTerm=open+mic+comedy+standup+comedia+espanol&geoRadius=10
extractor: eventfrog
listing_behavior: eventfrog
```

Nur diese Keys werden gelesen: `id`, `label`, `start_url`, `extractor`, `listing_behavior`.

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

```source
id: eventfrog_de
label: Eventfrog (DE, Zürich Comedy & Kabarett / Open Mic)
start_url: https://eventfrog.ch/de/events/zuerich/comedy-cabaret.html?searchTerm=open+mic+comedy+standup+comedia+espanol&geoRadius=10
extractor: eventfrog
listing_behavior: eventfrog
```

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

```source
id: zuerich_com_event_finder
label: Zürich Tourismus (Guidle microsite search)
start_url: https://microsite.guidle.com/api/rest/2.0/portals/search-offers/658578869?portalName=microsite&pageOfferId=1172134252&sectionId=1096&currentPageNumber=1&micrositeCrId=e8X87y&language=de&search=open+mic+standup+comedy
extractor: guidle_microsite
listing_behavior: none
```

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

```source
id: stubae_comedy
label: Stubä Comedy (Milanski Comedy)
start_url: https://www.milanski-comedy.ch/stubae-comedy
extractor: single_page
listing_behavior: none
```

---

## Geplant (Platzhalter)

| Name | Notiz |
|------|--------|
| *(noch keine weiteren Quellen)* | Nach Eventfrog: z. B. Veranstalter-Websites, ComedyHaus-Kalender, Meetup, Facebook-Events. |
