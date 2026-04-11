# Quellenliste — Open Mics Zürich

Übersicht der Datenquellen für Comedy- und Open-Mic-Events in und um Zürich.  
Einträge nach Priorität oder Startreihenfolge; später ergänzen.

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

## Geplant (Platzhalter)

| Name | Notiz |
|------|--------|
| *(noch keine weiteren Quellen)* | Nach Eventfrog: z. B. Veranstalter-Websites, ComedyHaus-Kalender, Meetup, Facebook-Events. |
