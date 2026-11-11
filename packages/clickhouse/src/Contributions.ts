import * as Contributions from "alchemy/Contributions";

export default Contributions.make({
  name: "clickhouse",
  auth: Contributions.lazy(
    () => import("./AuthProvider.ts"),
    (module) => module.ClickHouseAuth,
  ),
});
