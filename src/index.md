---
sql:
    AusData: aus.parquet
    data: OxCGRT_compact_national_v1.parquet
    metadata: metadata.parquet
---

# Oxcart Paradox

```js
const select = view(Inputs.select(policyTypes))
```
```js
// Eyeballed
const time = view(Inputs.range([20220201, 20221200], {step: 1, value: 20220201}))
```

```js
Plot.plot({
  projection: "equal-earth",
  width: 928,
  height: 928 / 2,
  color: {scheme: "YlGnBu", unknown: "#ccc", label: "Policy Value", legend: true},
  marks: [
    Plot.sphere({fill: "white", stroke: "currentColor"}),
    Plot.geo(countries, {
      fill: (map => d => map.get(d.properties.name))(new Map(filteredData.map(d => [d.CountryName, d.PolicyValue]))),
    }),
    Plot.geo(countrymesh, {stroke: "white"}),
 ]
})
```

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
AND Date = ${parseInt(time)}
```

<!-- Appendix -->

```sql id=[...metadata]
SELECT * FROM metadata
```

```js
const world = FileAttachment("countries-50m.json").json()
```
```js
const countries = topojson.feature(world, world.objects.countries)
const countrymesh = topojson.mesh(world, world.objects.countries, (a, b) => a !== b)
```

## Australian data


```sql id=[...ts_deaths]
SELECT RegionName, ConfirmedDeaths, CityName, Date 
FROM AusData 
WHERE ConfirmedDeaths NOT NULL AND Date > 20220000
ORDER BY Date 
```

```js
Plot.plot({
    y: {grid: true},
    x: {label: "time (hours)"},
    marginLeft: 70,
    color: {legend: true},
    title: "Confirmed death by Region Name in Australia",
    caption: "p.s. still need to know where the time starts",
    marks: [
        Plot.line(ts_deaths, {x: "Date", y: "ConfirmedDeaths", stroke: "RegionName"})
    ]
})
```
#### Raw data

```sql 
SELECT * FROM AusData 
WHERE RegionName='Queensland' AND CityName NOT NULL
```