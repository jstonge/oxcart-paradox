---
theme: near-midnight
toc: false
sql:
    data: ./data/OxCGRT_compact_national_v1.parquet
    metadata: ./data/metadata.parquet
---

# Oxcart data

## National level

```js
const selectInput = Inputs.select(policyTypes);
const select = Generators.input(selectInput);

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
            height: 150,
            width,
            x: {type: "utc"},
            y: {label: "weekly new case (total)"},
            marginLeft: 80,
            marks: [
                Plot.lineY(metadata, Plot.windowY({k: 7}, {x: "date", y: "new_cases"})),
                Plot.ruleX([new Date(time)], {stroke: "red"})
            ]
        }))}
    </div>
</div>
<div>${resize((width) =>
    Plot.plot({
    projection: "equal-earth",
    width,
    height: width / 2.4,
    color: {
        scheme: "YlGnBu", 
        unknown: "#ccc", 
        label: "Policy Value", legend: true,
        domain: [0, Policy2Val.get(select)],
        range: [0, 0.9]
    } ,
    marks: [
        Plot.sphere({stroke: "currentColor"}),
        Plot.geo(countries, {
            fill: (map => d => map.get(d.properties.name))(new Map(filteredData.map(d => [d.CountryName, d.PolicyValue])))
        }),
        Plot.geo(countrymesh, {stroke: "black", strokeOpacity: 0.3}),
    ]
    }))}
</div>

<br>

```sql id=[...raw_data]
SELECT PolicyType, MAX(PolicyValue) AS PolicyValue
FROM data
GROUP BY PolicyType;
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
const world = FileAttachment("./data/countries-50m.json").json()
```

```js
const policyTypes = new Set(raw_data.map(d=>d.PolicyType))
const Policy2Val = (new Map(raw_data.map(d => [d.PolicyType, d.PolicyValue])))

const countries = topojson.feature(world, world.objects.countries)
const countrymesh = topojson.mesh(world, world.objects.countries, (a, b) => a !== b)
```