## graphite aggregation backfiller.
Need I say more?

## Ok, maybe I do.
You can use carbon-aggregator or carbon-relay-ng's built-in aggregation, which will aggregate data as it comes in.

Typically you will want to backfill aggregated data by applying rules of said tools on historical data.
This tool uses the powerful aggregation library of [carbon-relay-ng](https://github.com/graphite-ng/carbon-relay-ng),
check that out for details on what you can do and syntax.
You can run this program with the same aggregation options as the ones you use in your carbon-relay-ng aggregation rules.

## Syntax

    $ ./graphite-aggregation-backfiller -h
    gab <graphite> <carbon> <regex> <out> <func> <from> <to>
        graphite: http://mygraphiteserver
        carbon: yourcarbonhost:2003
        regex: regex to match incoming metrics
        out:   pattern to construct outgoing metric
        func:  function to use. avg or sumk
        from:  from unix timestamp (default: 0)
        to:    to unix timestamp (default: now)

## Example

    $ ./graphite-aggregation-backfiller http://graphite graphite:2003 '^stats\.dc1[^.]+\.logger\.([^.]*)' 'stats._sum_dc1.logger.$1' sum $(date -d 'march 27' +%s) $(date -d 'march 29' +%s)


