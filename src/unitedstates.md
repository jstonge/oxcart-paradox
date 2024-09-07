---
theme: near-midnight
toc: false
sql:
    UnitedStates: ./data/US.parquet
    metadata: ./data/metadata.parquet
---

# United States

```js
const selectInput = Inputs.select(policyTypesUS);
const select = Generators.input(selectInput);

const timeInput = Inputs.date({label: "Day", value: "2021-09-21"});
const time = Generators.input(timeInput);
```

<div class="grid grid-cols-3">
    <div>
        Choose among policy types
        <br>
        ${selectInput}
        Then choose a day (you can click on date to toggle with keyboard):
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
        width,
        height: width / 2.4,
  projection: "albers-usa",
  color: {
    scheme: "YlGnBu", unknown: "#ccc", 
    label: "Policy Value", legend: true, 
    domain: [0, Policy2Val.get(select)]
},
  marks: [
     Plot.geo(states, {stroke: "black",
        fill: (map => d => map.get(d.properties.name))(new Map(filteredData_us.map(d => [d.RegionName, d.PolicyValue]))),
        })
  ]
})
)}
</div>

```sql id=[...metadata]
SELECT SUM(new_cases) as new_cases, date 
FROM metadata WHERE country = 'United States'
GROUP BY date 
ORDER BY date
```

```sql id=[...filteredData_us]
SELECT RegionName, Date, PolicyType, PolicyValue 
FROM UnitedStates
WHERE PolicyType = ${select}
AND Date = ${time.toISOString().slice(0, 10)}
```

```sql id=[...UnitedStates]
SELECT PolicyType, MAX(PolicyValue) AS PolicyValue
FROM UnitedStates
GROUP BY PolicyType;
```
```js
const us = FileAttachment("./data/us-counties-10m.json").json()
```

```js
const nation = topojson.feature(us, us.objects.nation)
const states = topojson.feature(us, us.objects.states).features
const policyTypesUS = new Set(UnitedStates.map(d=>d.PolicyType))
const Policy2Val = (new Map(UnitedStates.map(d => [d.PolicyType, d.PolicyValue])))
```