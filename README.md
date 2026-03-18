# Getdown

一个用 Go 写的“数据挖掘/下载”小工具：下载 **TCGA（优先 UCSC Xena hub，支持多组学；可选 GDC）** 和 **GEO（GSE series matrix；必要时自动回退 supplementary；芯片数据额外下载平台注释）**，并把结果落到本地 TSV 文件，方便后续在 R / Python 里继续分析。
现在也支持 **SRA（SRR/SRP/... 检索；通过公开下载链接直接下载 run 文件）**。

> 仓库里原来的 R 版本逻辑在 `moriaclass.R`。

## 安装 / 构建

```bash
go test ./...
go build ./cmd/getdown
```

生成的二进制为 `./getdown`。

## 用法

### TCGA

默认走 Xena hub 的 `/data/` API：会把该项目（如 `TCGA-LAML.*`）在 hub 上能找到的数据集 **全部下载**（多组学 + 同一组学的多资源）。如需只走 GDC 或自动回退可用 `--provider` 控制。

```bash
./getdown tcga --project TCGA-LAML --out ./out/tcga_laml
```

可选参数：

- `--provider xena|auto|gdc`：默认 `xena`（`auto` 表示 Xena 失败再回退 GDC）
- `--xena-mode all|core`：默认 `all`；`core` 只下载表达矩阵 + 临床（适合 CI 或快速验证）
- `--workflow "STAR - Counts"`：GDC 表达量 workflow（默认同 R 脚本）
- `--keep-raw`：保留下载的原始文件到 `out/raw/...`

输出（写入 `--out` 目录）：

- `expression.tsv`：基因 × 样本的表达矩阵（TSV）
- `clinical.tsv`：临床/表型信息（TSV）
- `omics/`：Xena 模式下下载的全部数据集（文件名≈dataset name；矩阵/表格均写为 TSV）
- `metadata.json`：记录本次下载的参数与实际使用的数据源（gdc 或 xena）

### GEO

优先下载并解析 `GSE*_series_matrix.txt.gz`（如果存在）。

- 若发现 series matrix 的表格区只有表头（没有数据行），会自动回退下载 supplementary files（即使未传 `--sup`）。
- 若 `!Series_type` 显示为 array/chip（芯片）且能解析到 `GPL...`，会额外下载平台注释文件到 `platform/`。

```bash
./getdown geo --gse GSE13535 --out ./out/gse13535 --sup
```

输出（写入 `--out` 目录）：

- `expression.tsv`：series matrix 表达矩阵（原样写出）
- `sample_kv.tsv`：`!Sample_*` 头信息（按样本列展开；同名字段会追加 `#2/#3...`）
- `series_kv.tsv`：`!Series_*` 头信息（长表：field/value）
- `supplementary/`：`--sup` 时下载的补充文件
- `platform/`：芯片数据时下载的 GPL 注释（如 `GPL570.annot.gz`）
- `metadata.json`

### Search

对输入的 `GSE...` / `SRR/SRP/...` / `TCGA-...` 进行快速检索（存在性 + 基本信息），或按关键词同时检索 `GEO + SRA + GDC(TCGA) + Xena`：

```bash
./getdown search GSE235527
./getdown search SRR123456
./getdown search TCGA-LAML
./getdown search leukemia
./getdown search --source geo alzheimer
./getdown search --source sra leukemia
./getdown search --source tcga leukemia
./getdown search --source xena leukemia
```

说明：

- `GSE...`：使用 NCBI E-utilities（`esearch/esummary`）查询 GEO 信息。
- `SRR/SRP/...`：使用 NCBI E-utilities 做检索；精确 accession 查询时也会解析 run 信息。
- `TCGA-...`：默认会同时查询 GDC 项目和 Xena hub；返回结果里会分别标记为 `source=tcga` 与 `source=xena`。
- `xena` 关键词检索：直接查 hub 上的 dataset 名称和 `longtitle`，适合找组学资源、矩阵名、cohort 相关条目。
- `--source` 支持 `all|geo|sra|tcga|xena`；默认 `all`。
- 输入大小写不敏感（例如 `gse235527` / `tcga-laml` 都可）。

### SRA

支持对 `SRR/SRX/SRS/SRP`（以及 `ERR/ERP/DRR/DRP` 等同类 accession）进行下载。

```bash
./getdown sra --accession SRR123456 --out ./out/srr123456
./getdown sra --accession SRP012345 --out ./out/srp012345
./getdown sra --accession SRR123456 --kind fastq --out ./out/srr123456
```

说明：

- 下载前会先解析 run 列表并写出 `runinfo.tsv` 和 `links.tsv`。
- 当前版本通过公开下载链接直接下载，不依赖 `SRA Toolkit`。
- `--kind auto|fastq|submitted|sra|all`
- `auto` 默认优先级：`fastq` > `submitted` > `sra`
- 文件会保存到 `files/<run_accession>/...`
- 会写出 `metadata.json`

## 说明 / 局限

- Xena 默认会下载同一项目的所有 `TCGA-XXX.*` 数据集；不同项目/Hub 的可用数据集差异很大，下载量也可能非常大。
- 若 hub `/data/` API 不可用，会回退为“静态镜像”方式（只保证能尽力拿到 `expression.tsv`/`clinical.tsv`，不保证多组学）。
- GEO 的 supplementary 下载依赖文件里提供的 URL（常见为 `!Series_supplementary_file` / `!Sample_supplementary_file`）；有些 GSE 的 series matrix 本身不包含表达矩阵，需要依赖 supplementary（例如 RAW tar）。
