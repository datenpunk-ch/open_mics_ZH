<!--
  Article body in Markdown.
  This file is rendered into `docs/index.html` at runtime.
  (Raw HTML blocks are allowed and will be passed through.)
-->

<section>
  <div class="container">
    <div class="section-label reveal">Start here</div>
    <div class="sec-head reveal">
      <div class="sec-num">01</div>
      <h2>What this project is (and isn’t)</h2>
    </div>
    <div class="measure">

**Open MicZH** is a curated list of **recurring** open mic comedy events in Zurich. “Recurring” means: if you go next week, there’s a good chance it’s happening again.

It’s not a full calendar of every one-off showcase. It’s not a review site. It’s meant to be useful when you have a free evening and want an event you can trust is really there.

      <div class="note-box reveal">
        <div class="note-box-label">Tip</div>
        <p>
          Use the filters to pick a weekday/language, then click an event in the list — the map will jump
          to that venue and show the tooltip.
        </p>
      </div>

      <div class="stats reveal" id="langStats" aria-live="polite">
        <div class="stats-label">Shows by language</div>
        <div class="stats-cards" id="langStatsCards">
          <div class="stat-card is-loading">Loading…</div>
        </div>
      </div>

      <div class="stats reveal" id="weekdayStats" aria-live="polite">
        <div class="stats-label">Shows by weekday</div>
        <p class="weekday-strip-hint">
          Bar height is relative to the busiest night in this dataset (same show on two days counts twice).
        </p>
        <div id="weekdayStatsInner">
          <div class="stat-card is-loading" style="width:100%;max-width:100%;height:auto;min-height:72px;">
            Loading…
          </div>
        </div>
      </div>

      <div class="stats reveal" id="manualStats" aria-live="polite">
        <div class="stats-label">Data status &amp; venue overrides</div>
        <div class="stats-cards" id="manualStatsCards">
          <div class="stat-card is-loading">Loading…</div>
        </div>
      </div>

    </div>
  </div>
</section>

<section>
  <div class="container">
    <div class="section-label reveal">How to use it</div>
    <div class="sec-head reveal">
      <div class="sec-num">02</div>
      <h2>From “I’m curious” to “I’m on the mic”</h2>
    </div>
    <div class="measure">

### If you’re going to watch

- **Arrive early** for a seat and to avoid interrupting sets.
- **Check the venue link** (each entry links to the organizer/venue page when available).
- **Respect the room**: phones down, listening up.

### If you’re going to perform

- **Know the format**: sign-up style and set length vary by event.
- **Bring a tight 3–5**: one clear premise is better than five half-jokes.
- **Talk to the host**: they control the flow, and they can answer practical questions fast.

    </div>
  </div>
</section>

<section id="interactive">
  <div class="container">
    <div class="section-label reveal">Interactive</div>
    <div class="sec-head reveal">
      <div class="sec-num">03</div>
      <h2>The map + list, embedded</h2>
    </div>
  </div>
  <div class="wide-bleed">
    <div class="embed-frame reveal" style="height: min(64vh, 700px);">
      <iframe
        title="Open MicZH interactive map and list"
        src="./map.html"
        loading="lazy"
        referrerpolicy="no-referrer"
      ></iframe>
    </div>
    <div class="container">
      <div class="measure">
        <div class="note-box reveal" style="margin-top: 1.5rem;">
          <div class="note-box-label">Note</div>
          <p>
            If the embed feels cramped on mobile, open the full map page instead:
            <a href="./map.html">Open MicZH map</a>.
          </p>
        </div>
      </div>
    </div>
  </div>
</section>

<section>
  <div class="container">
    <div class="section-label reveal">Contribute</div>
    <div class="sec-head reveal">
      <div class="sec-num">04</div>
      <h2>How to keep the list accurate</h2>
    </div>
    <div class="measure">

Scenes change quickly: venues close, formats rotate, organizers take breaks. If you spot an issue, the fastest way to improve the site is to open a GitHub issue with the exact correction.

- **Wrong location?** Share the venue name + address.
- **Time changed?** Provide the updated start time and a source link.
- **New recurring mic?** Include weekday, language(s), and organizer link.

Project repo: [open_mics_ZH](https://github.com/datenpunk-ch/open_mics_ZH).

    </div>
  </div>
</section>
