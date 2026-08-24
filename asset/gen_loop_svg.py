#!/usr/bin/env python3
"""Generate asset/loop-light.svg and asset/loop-dark.svg — the Observe→Learn→Act
triangle cycle for the README hero. One generator, two palettes, zero drift."""

FONT = "Inter, system-ui, -apple-system, 'Segoe UI', Roboto, sans-serif"
MONO = "ui-monospace, SFMono-Regular, 'SF Mono', Menlo, Consolas, monospace"

PALETTES = {
    "light": dict(
        card_fill="#f6f8fa",
        card_stroke="#d1d9e0",
        title="#1f2328",
        body="#59636e",
        mono="#0b6e9e",
        badge="#0b8fd1",
        badge_text="#ffffff",
        arrow="#6a6f7a",
        center="#bf6a02",
    ),
    "dark": dict(
        card_fill="#161b22",
        card_stroke="#3d444d",
        title="#f0f6fc",
        body="#9198a1",
        mono="#4db8ec",
        badge="#0b8fd1",
        badge_text="#ffffff",
        arrow="#9198a1",
        center="#e8a300",
    ),
}

W, H = 1280, 640
CARD_W, CARD_H = 400, 172

CARDS = [
    # (num, title, cx, cy, body_lines, mono_line)
    (
        "1",
        "OBSERVE",
        640,
        40,
        ["the agent asks anything, in SQL,", "and declares why"],
        "POST /query + ai_context → audit ledger",
    ),
    (
        "2",
        "LEARN",
        856,
        428,
        [
            "read the ledger, group by session",
            "and purpose — find the intentions",
            "that keep coming back",
        ],
        None,
    ),
    (
        "3",
        "ACT",
        24,
        428,
        [
            "recurring queries → named pipelines;",
            "recurring intentions → routines",
            "that run before they're asked",
        ],
        None,
    ),
]
# cx above is the card's left x for LEARN/ACT; for OBSERVE it's the center x.
CARDS[0] = ("1", "OBSERVE", 640 - CARD_W // 2, 40, CARDS[0][4], CARDS[0][5])


def esc(s):
    return s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def card(p, num, title, x, y, lines, mono_line):
    out = []
    out.append(
        f'<rect x="{x}" y="{y}" width="{CARD_W}" height="{CARD_H}" rx="12" '
        f'fill="{p["card_fill"]}" stroke="{p["card_stroke"]}" stroke-width="1.5"/>'
    )
    bx, by = x + 38, y + 42
    out.append(f'<circle cx="{bx}" cy="{by}" r="16" fill="{p["badge"]}"/>')
    out.append(
        f'<text x="{bx}" y="{by + 5.5}" text-anchor="middle" font-family="{FONT}" '
        f'font-size="16" font-weight="700" fill="{p["badge_text"]}">{num}</text>'
    )
    out.append(
        f'<text x="{bx + 28}" y="{by + 7}" font-family="{FONT}" font-size="21" '
        f'font-weight="700" letter-spacing="1.5" fill="{p["title"]}">{title}</text>'
    )
    ty = y + 86
    for line in lines:
        out.append(
            f'<text x="{x + CARD_W / 2}" y="{ty}" text-anchor="middle" '
            f'font-family="{FONT}" font-size="17" fill="{p["body"]}">{esc(line)}</text>'
        )
        ty += 26
    if mono_line:
        out.append(
            f'<text x="{x + CARD_W / 2}" y="{ty + 4}" text-anchor="middle" '
            f'font-family="{MONO}" font-size="14" fill="{p["mono"]}">{esc(mono_line)}</text>'
        )
    return "\n".join(out)


def render(theme):
    p = PALETTES[theme]
    parts = [
        f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {W} {H}" '
        f'font-family="{FONT}" role="img" '
        'aria-label="The loop: observe, learn, act — the toolset grew itself">',
        "<defs>",
        f'<marker id="arr" viewBox="0 0 10 10" refX="8" refY="5" markerWidth="7" '
        f'markerHeight="7" orient="auto-start-reverse">'
        f'<path d="M 0 1 L 9 5 L 0 9 z" fill="{p["arrow"]}"/></marker>',
        f'<marker id="arr2" viewBox="0 0 10 10" refX="8" refY="5" markerWidth="6" '
        f'markerHeight="6" orient="auto-start-reverse">'
        f'<path d="M 0 1 L 9 5 L 0 9 z" fill="{p["center"]}"/></marker>',
        "</defs>",
    ]
    for c in CARDS:
        parts.append(card(p, *c))

    arrow_style = (
        f'fill="none" stroke="{p["arrow"]}" stroke-width="2.5" marker-end="url(#arr)"'
    )
    # OBSERVE (right edge) -> LEARN (top edge), clockwise
    parts.append(f'<path d="M 846 150 Q 1078 208 1058 420" {arrow_style}/>')
    # LEARN (left edge) -> ACT (right edge), along the bottom
    parts.append(f'<path d="M 848 514 Q 640 562 436 514" {arrow_style}/>')
    # ACT (top edge) -> OBSERVE (left edge), closing the cycle
    parts.append(f'<path d="M 222 420 Q 202 208 432 152" {arrow_style}/>')

    # centre: the payoff line, with a cycle glyph
    parts.append(
        f'<g stroke="{p["center"]}" stroke-width="2.5" fill="none">'
        f'<path d="M 621 316 A 22 22 0 1 1 640 349" marker-end="url(#arr2)"/></g>'
    )
    parts.append(
        f'<text x="640" y="396" text-anchor="middle" font-family="{FONT}" '
        f'font-size="19" font-style="italic" fill="{p["center"]}">the toolset grew itself</text>'
    )
    parts.append("</svg>")
    return "\n".join(parts)


for theme in ("light", "dark"):
    path = f"asset/loop-{theme}.svg"
    with open(path, "w") as f:
        f.write(render(theme))
    print(f"wrote {path}")
