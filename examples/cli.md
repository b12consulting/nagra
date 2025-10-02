

# Nagra command line interface

Nagra comes with a limited but handy command line, it allows to
explore databases (created by nagra or by other system).

The examples hereunder make the assumption that you have ran the
[Fastapi](https://github.com/b12consulting/nagra/blob/master/examples/fastapi-example.py)
example, and to have a file `example.db` in the curret folder. In
order to do so, you can simply run `python fastapi-example.py`.


## Setup & schema
first define the `NAGRA_DB` environment variable. One can also pass
the `--db` argument, if you have frequently switch from one db to the
other.

``` sh
$ export NAGRA_DB=sqlite://example.db
```

You can now rely on introspection in order to get the db schema:

``` sh
$ nagra schema
╭─────────────┬───────╮
│ table       │ view  │
├─────────────┼───────┤
│ city        │ False │
│ temperature │ False │
╰─────────────┴───────╯
$ nagra schema temperature
╭─────────────┬───────────┬───────────╮
│ table       │ column    │ type      │
├─────────────┼───────────┼───────────┤
│ temperature │ id        │ int       │
│ temperature │ city      │ int       │
│ temperature │ timestamp │ timestamp │
│ temperature │ value     │ float     │
╰─────────────┴───────────┴───────────╯
```


## Select

You can use the select command and use the dotted notation in order to
resolve foreign keys

``` sh
$ nagra select temperature  # with only the table name given, we select all columns 
╭────┬──────┬──────────────────┬───────╮
│ id │ city │ timestamp        │ value │
├────┼──────┼──────────────────┼───────┤
│ 1  │ 1    │ 2000-01-01T00:00 │ 0.0   │
│ 2  │ 2    │ 2000-01-01T00:00 │ 1.0   │
│ 3  │ 3    │ 2000-01-01T00:00 │ 3.0   │
│ 4  │ 3    │ 2000-01-01T01:00 │ 2.0   │
╰────┴──────┴──────────────────┴───────╯
$ nagra select temperature city.name timestamp value  # every subsequent argument is treated as a column name
╭───────────┬──────────────────┬───────╮
│ city.name │ timestamp        │ value │
├───────────┼──────────────────┼───────┤
│ Brussels  │ 2000-01-01T00:00 │ 0.0   │
│ London    │ 2000-01-01T00:00 │ 1.0   │
│ Berlin    │ 2000-01-01T00:00 │ 3.0   │
│ Berlin    │ 2000-01-01T01:00 │ 2.0   │
╰───────────┴──────────────────┴───────╯
$ nagra select temperature timestamp value city.name=Brussels  # .. except if it contains an "=", it is then a filter 
╭──────────────────┬───────╮
│ timestamp        │ value │
├──────────────────┼───────┤
│ 2000-01-01T00:00 │ 0.0   │
╰──────────────────┴───────╯
```

Advanced filters can be given wth the `-W` argument, order by with `-O`:

``` sh
$ nagra select temperature timestamp value  -W'(>= value 2)' -O value
╭──────────────────┬───────╮
│ timestamp        │ value │
├──────────────────┼───────┤
│ 2000-01-01T01:00 │ 2.0   │
│ 2000-01-01T00:00 │ 3.0   │
╰──────────────────┴───────╯
```

Expression can be used as selected column too (Sqlite returns boolean values as 1/0):

``` sh
$ nagra select temperature '(= value 2)'
╭─────────────╮
│ (= value 2) │
├─────────────┤
│ 0           │
│ 0           │
│ 0           │
│ 1           │
╰─────────────╯
```

When an aggregate expression is given, groupby is automatically
enabled on other columns:

``` sh
$ nagra select temperature city.name '(max value)'
╭───────────┬─────────────╮
│ city.name │ (max value) │
├───────────┼─────────────┤
│ Berlin    │ 3.0         │
│ Brussels  │ 0.0         │
│ London    │ 1.0         │
╰───────────┴─────────────╯
```

Results can be returned in "pivot mode", or as csv:

``` sh
$ nagra -p select temperature city.name '(max value)'


 ──────────────────────
  city.name     Berlin
  (max value)   3.0


 ────────────────────────
  city.name     Brussels
  (max value)   0.0


 ──────────────────────
  city.name     London
  (max value)   1.0
```

``` sh
$ nagra --csv  select temperature city.name '(max value)'
city.name,(max value)
Berlin,3.0
Brussels,0.0
London,1.0
```


## Delete

There is also a delete command, it also supports the `-W` flag (but not the compact version) :

``` sh
$ nagra delete city
```

Beware that foreign keys are all defined with "ON DELETE CASCADE" pragma

``` sh
$ nagra select temperature
╭────┬──────┬───────────┬───────╮
│ id │ city │ timestamp │ value │
├────┼──────┼───────────┼───────┤
╰────┴──────┴───────────┴───────╯
```
