# Parse only the selected release; preserve original lines when rendering.
# Modes: level, stamp (Unreleased), notes (version).
function fail(message) {
    print "changelog: " message > "/dev/stderr"
    failed = 1
    exit 1
}
function content(line, cleaned) {
    cleaned = line
    # Comments are placeholders, not release content.
    while (length(cleaned)) {
        if (comment) {
            if (!index(cleaned, "-->")) return
            cleaned = substr(cleaned, index(cleaned, "-->") + 3)
            comment = 0
        } else if (index(cleaned, "<!--")) {
            content(substr(cleaned, 1, index(cleaned, "<!--") - 1))
            cleaned = substr(cleaned, index(cleaned, "<!--") + 4)
            comment = 1
        } else {
            if (cleaned ~ /[^[:space:]]/ && cleaned !~ /^[[:space:]]*[-*+][[:space:]]*$/) {
                if (!category) fail("content must belong to a category")
                populated[category] = 1
            }
            return
        }
    }
}
BEGIN {
    rank["Breaking changes"] = 3
    rank["Added"] = rank["Changed"] = 2
    rank["Fixed"] = rank["Security"] = rank["Documentation"] = 1
    rank["Upgrade notes"] = 0
    wanted = mode == "notes" ? "## [" version "] - " : "## [Unreleased]"
}
{
    lines[NR] = $0
    line = $0
    if (active && comment) { content(line); next }
    marker = line
    sub(/^ ? ? ?/, "", marker)
    if (fence != "") {
        run = marker
        sub(fence == "`" ? "[^`].*$" : "[^~].*$", "", run)
        tail = substr(marker, length(run) + 1)
        if (substr(marker, 1, 1) == fence && length(run) >= fence_length && tail ~ /^[[:space:]]*$/) {
            fence = ""
        } else if (active && line ~ /[^[:space:]]/) {
            if (!category) fail("code must belong to a category")
            populated[category] = 1
        }
        next
    }
    if (marker ~ /^(```|~~~)/) {
        fence = substr(marker, 1, 1)
        run = marker
        sub(fence == "`" ? "[^`].*$" : "[^~].*$", "", run)
        fence_length = length(run)
        next
    }
    if (line ~ /^##[[:space:]]/) {
        if (active) { finish = NR - 1; active = 0 }
        matches = mode == "notes" ? index(line, wanted) == 1 : line == wanted
        if (matches) {
            if (++found > 1) fail("duplicate selected release heading")
            if (mode != "notes" && releases) fail("Unreleased must be the first release")
            if (mode == "notes" && substr(line, length(wanted) + 1) !~ /^[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]$/) fail("invalid release date heading")
            start = NR
            active = 1
        }
        releases++
        next
    }
    if (!active) next
    if (line ~ /^###[[:space:]]/) {
        category = substr(line, 5)
        if (!(category in rank)) fail("unknown category: " category)
        if (seen[category]++) fail("duplicate category: " category)
        next
    }
    if (line ~ /^####[#]*[[:space:]]/) next
    content(line)
}
END {
    if (failed) exit 1
    if (found != 1) fail("expected exactly one " wanted " heading")
    if (active) finish = NR
    if (active && fence != "") fail("unclosed code fence")
    if (comment) fail("unclosed comment")
    for (name in populated) if (rank[name] > level) level = rank[name]
    if (!level) fail("no releasable changes (Upgrade notes alone are insufficient)")
    if (mode == "level") print level
    else if (mode == "notes") {
        for (i = start + 1; i <= finish; i++) print lines[i]
    } else if (mode == "stamp") {
        for (i = 1; i <= NR; i++) {
            if (i == start) {
                print "## [Unreleased]\n"
                print "### Breaking changes\n\n### Added\n\n### Changed\n\n### Fixed\n\n### Security\n\n### Documentation\n\n### Upgrade notes\n"
                print "## [" version "] - " date
            } else {
                line = lines[i]
                if (i > start && i <= finish) {
                    gsub(/\{\{version\}\}/, version, line)
                    gsub(/\{\{date\}\}/, date, line)
                }
                print line
            }
        }
    } else fail("unknown mode")
}
