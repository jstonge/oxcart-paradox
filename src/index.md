---
sql:
    AusData: aus.parquet
    data: OxCGRT_compact_national_v1.parquet
    metadata: metadata.parquet
---

# Oxcart Paradox

We inspect the oxcart data at National level, as well as the subnational level for the US.

## National level

```js
const selectInput = Inputs.select(policyTypes);
const select = Generators.input(selectInput);
```
```js
const timeInput = Inputs.date({label: "Day", value: "2021-09-21"});
const time = Generators.input(timeInput);
```
<div class="grid grid-cols-3">
    <div>
        We first choose among policy types
        ${selectInput}
        <br>
        Then choose a day:
        ${timeInput}
    </div>
    <div class="grid-colspan-2">
        ${resize((width) => Plot.plot({
            height: 200,
            width,
            x: {type: "utc"},
            y: {label: "weekly new case (total)"},
            marginLeft: 80,
            marks: [
                Plot.lineY(metadata, Plot.windowY({k: 7}, {x: "date", y: "new_cases"}))
            ]
        }))}
    </div>
</div>
<div>${resize((width) =>
    Plot.plot({
    projection: "equal-earth",
    width: width,
    height: width / 2.3,
    color: {scheme: "YlGnBu", unknown: "#ccc", label: "Policy Value", legend: true},
    marks: [
        Plot.sphere({fill: "white", stroke: "currentColor"}),
        Plot.geo(countries, {
        fill: (map => d => map.get(d.properties.name))(new Map(filteredData.map(d => [d.CountryName, d.PolicyValue]))),
        }),
        Plot.geo(countrymesh, {stroke: "white"}),
    ]
    }))}
</div>

<br>

## National compact data

```sql id=[...raw_data]
SELECT * FROM data
```
```js
Inputs.table(raw_data)
```

```js
const policyTypes = new Set(raw_data.map(d=>d.PolicyType))
```

```sql id=[...filteredData]
SELECT CountryName, PolicyType, PolicyValue 
FROM data 
WHERE PolicyType = ${select}
AND Date = ${time.toISOString().slice(0, 10)}
```

```sql id=[...metadata]
SELECT SUM(new_cases) as new_cases, date 
FROM metadata 
GROUP BY date 
ORDER BY date
```

```js
const world = FileAttachment("countries-50m.json").json()
```
```js
const countries = topojson.feature(world, world.objects.countries)
const countrymesh = topojson.mesh(world, world.objects.countries, (a, b) => a !== b)
```
