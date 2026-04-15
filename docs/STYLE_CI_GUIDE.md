# Style Guide 

---

## Design intent

- **Editorial, typography-led**
  - Pages should read like a magazine spread: hierarchy, rhythm, restraint.
- **No “startup UI”**
  - Avoid cards, shadows, rounded corners, glass, gradients, decorative blobs.
- **Rules over boxes**
  - Use hairline rules and spacing to structure content, not heavy containers.
- **One accent, used sparingly**
  - Accent is for links, counters, and focus states—never as a second body color.

---

## Palette (cool + high contrast)

Use a strict set of roles:

- **Background**: white / near-white
- **Ink**: near-black (not pure black; reduce glare)
- **Body ink**: slightly softer than headings
- **Muted**: for secondary text
- **Rule**: cool grey hairlines
- **Accent**: one cool color used for emphasis and focus

### Recommended hex palette (copy/paste)

These values match the current editorial palette:

- **Background**: `#ffffff`
- **Background (soft)**: `#f7f8f9`
- **Ink (headings/links default)**: `#0f0f0f`
- **Body ink**: `#171717`
- **Muted**: `#5f5f5f`
- **Rule (hairlines)**: `#d9dde1`
- **Accent**: `#3a677a`
- **Accent (hover)**: `#2f5a6b`

Optional CSS token block:

```css
:root{
  --color-bg:#ffffff;
  --color-bg-soft:#f7f8f9;
  --color-ink:#0f0f0f;
  --color-ink-body:#171717;
  --color-muted:#5f5f5f;
  --color-rule:#d9dde1;
  --color-accent:#3a677a;
  --color-accent-hover:#2f5a6b;
}
```

Guidelines:

- Prefer **cool greys** (blue-grey) for rules; avoid beige/cream “paper” tones.
- Avoid multiple accent hues; if you need more, use **weight/scale/spacing** first.

---

## Typography

### Font roles

- **Display/headlines**: high‑contrast serif (e.g. Spectral)
- **UI/body**: neutral grotesk (e.g. Karla)
- **Mono**: for technical hints, code-ish links, metadata (e.g. JetBrains Mono)

### Recommended font stack (copy/paste)

- **Display**: `"Spectral", "Georgia", serif`
- **UI/body**: `"Karla", system-ui, sans-serif`
- **Mono**: `"JetBrains Mono", ui-monospace, monospace`

Optional CSS token block:

```css
:root{
  --font-display:"Spectral","Georgia",serif;
  --font-ui:"Karla",system-ui,sans-serif;
  --font-mono:"JetBrains Mono",ui-monospace,monospace;
}
```

### Hierarchy

- **H1**: oversized, tight line-height, short measure (~10–12 characters wide).
- **Section headings (H2)**: serif, but paired with a small UI-label counter.
- **UI labels**: uppercase, tracked (higher letter-spacing), small size.

### Rhythm

- Body uses **relaxed line-height**.
- Use **short paragraphs** and strong spacing to reduce visual noise.

---

## Spacing & layout rules

### Measures (readability)

- Constrain long text to a **measure** (e.g. ~42rem).
- Let layout widen for chrome and lists, but keep paragraphs readable.

### Gutters (editorial asymmetry)

- Use asymmetric page gutters (start/end) for a subtle “print” feel.

### Sections

- Large vertical spacing between major sections.
- Use rules (borders) as separators: top borders for lists, bottom borders for headers.

---

## Interaction & accessibility

- **Focus**: visible `:focus-visible` outline in the accent color.
- **Hover**: subtle color shift; do not animate large layout changes.
- **Skip link**: always present for keyboard/screen-reader users.
- Prefer semantic HTML; do not rely on color alone to convey state.

---

## Component patterns (visual, reusable)

### Header / navigation

- Header is minimal: title left, nav right.
- Nav is uppercase UI text with letter-spacing.
- Active page indicated by **ink + accent underline**, not pills.

### Front masthead (“opening spread”)

- Large headline (serif) + short tagline (UI font).
- Optional lede text that can sit in a second column on wide screens, separated by a rule.

### Unnumbered preface block

- A small uppercase label + hairline rule
- One paragraph of body copy
- Purpose: add context without starting the numbered sequence.

### Numbered sections

- A counter (e.g. 01, 02…) appears before the H2.
- The counter is UI font, small, tracked, accent color.
- H2 uses display serif; section body uses UI font.

### Ruled teaser list (projects / articles)

- List uses a **top rule**; each item uses a **bottom hairline**.
- Item layout is flat (no card container).
- Typography pattern:
  - headline (serif)
  - deck (muted UI/body)
  - small uppercase “read” line (accent)

Optional thumbnail variant:

- Thumbnail sits to the left; text sits to the right.
- Thumbnail uses a fixed aspect ratio (e.g. 4:3) with `object-fit: cover`.
- On small screens, stack thumbnail above text.

### Figures

- Images should feel “printed”: no heavy frames.
- Caption is muted, smaller, and can be italic.

---

## Reuse checklist (style-only)

1. Define tokens for **color, type, spacing, measure** up front.
2. Establish the **editorial rules**: ruled lists, counters, uppercase UI labels.
3. Keep a strict hierarchy: **serif for headlines**, **sans for UI/body**, **mono for metadata**.
4. Keep the accent rare; let **ink + rule + whitespace** do the heavy lifting.
5. Validate keyboard focus and skip link on every page.

