import fs from "fs";
import path from "path";
import csv from "csv-parser";
import { createObjectCsvWriter } from "csv-writer";

async function main() {
  console.log("Loading silver data for aggregation...");
  const infile = path.join(process.cwd(), "data", "silver", "covid_clean.csv");
  const agg = {};

  fs.createReadStream(infile)
    .pipe(csv())
    .on("data", (r) => {
      const date = r.notification_date;
      if (!date) return;
      const month = date.slice(0, 7);
      const key = `${r.state}|${month}`;
      if (!agg[key])
        agg[key] = { covid_uti_sum: 0, covid_cli_sum: 0, deaths: 0, count: 0 };
      const a = agg[key];
      a.covid_uti_sum += parseInt(r.covid_uti || "0");
      a.covid_cli_sum += parseInt(r.covid_cli || "0");
      a.deaths += parseInt(r.confirmed_deaths || "0");
      a.count++;
    })
    .on("end", async () => {
      const outRows = [];

      for (const [key, v] of Object.entries(agg)) {
        const [state, ym] = key.split("|");
        outRows.push({
          state,
          year_month: ym,
          avg_covid_uti: (v.covid_uti_sum / v.count).toFixed(2),
          avg_covid_cli: (v.covid_cli_sum / v.count).toFixed(2),
          total_deaths: v.deaths,
        });
      }

      outRows.sort((a, b) => a.state.localeCompare(b.state));

      const writer = createObjectCsvWriter({
        path: path.join(process.cwd(), "data", "gold", "covid_summary.csv"),
        header: [
          { id: "state", title: "state" },
          { id: "year_month", title: "year_month" },
          { id: "avg_covid_uti", title: "avg_covid_uti" },
          { id: "avg_covid_cli", title: "avg_covid_cli" },
          { id: "total_deaths", title: "total_deaths" },
        ],
      });

      await writer.writeRecords(outRows);
      console.log(
        "Gold layer written:",
        outRows.length,
        "rows (sorted by state)"
      );
    });
}

main().catch((e) => console.error(e));
