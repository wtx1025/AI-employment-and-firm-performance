# Replication Guide  
This guide documents the data sources, variable construction, and step-by-step procedures used to build a firm-level AI measure from job postings and résumé data. Our methodology follows [Babina et al. (2024)](https://www.sciencedirect.com/science/article/pii/S0304405X2300185X), and we replicate Figure 1(b) from the paper.  
## 1. Job Postings  
In this section, we detail how we construct the firm-level AI measure from job-postings data. We begin with a description of the data, then outline the procedures and explain the purpose and functions of each code file. Finally, we present the replication results.  
### 1.1 Data Description  
We use job-posting data purchased from Lightcast. The dataset contains standard fields such as company name, job title, and required skills, spanning 2010–2025. Because the raw pulls from Lightcast arrive as multiple extracts, we reorganize them with basic OS-level file operations. Specifically, we create a top-level directory `jobs_by_year` with subdirectories `2010`, `2011`, …, `2025`; each subdirectory holds multiple Parquet files with the original records. This structure makes it easier to manage and process the large volume of data. Below, we report the file structure and the record counts by year:  
<details>
  <summary><b>jobs_by_year/</b></summary>
  <ul>
    <li>
      <details>
        <summary><code>2010/</code></summary>
        <ul>
          <li><code>parquet/</code>
            <ul>
              <li><code>part.0.parquet</code></li>
              <li><code>part.1.parquet</code></li>
            </ul>
          </li>
        </ul>
      </details>
    </li>
    <li>
      <details>
        <summary><code>2011/</code></summary>
        <ul>
          <li><code>parquet/</code>
            <ul>
              <li><code>part.0.parquet</code></li>
              <li><code>part.1.parquet</code></li>
            </ul>
          </li>
        </ul>
      </details>
    </li>
    <li><code>...</code></li>
  </ul>
</details>
<details>
  <summary><b>Yearly record counts (2010–2025)</b></summary>

| Year | Count        |
|:----:|--------------|
| 2010 | 12462425     |
| 2011 | 15265548     |
| 2012 | 15075969     |
| 2013 | 20810346     |
| 2014 | 22389352     |
| 2015 | 24939638     |
| 2016 | 26388936     |
| 2017 | 26249858     |
| 2018 | 33194240     |
| 2019 | 34490965     |
| 2020 | 34732350     |
| 2021 | 43723499     |
| 2022 | 48610981     |
| 2023 | 38914101     |
| 2024 | 36845527     |
| 2025 | 21877827     |

</details>  

### 1.2 Procedures  
1. Explode each record by parsing the `skills_name` field (e.g., pipe-delimited) to create one row per (job_id, skill).  
   <details>
      <summary><b>Example: before → after</b></summary>

    **Before (one row, pipe-delimited skills)**

    | ID    | skills_name          |
    |:----: |----------------------|
    | 12345 | `Python\|Stata\|C++` |

    **After (exploded to one row per skill)**

    | ID    | skills_name |
    |:----: |------------|
    | 12345 | Python     |
    | 12345 | Stata      |
    | 12345 | C++        |

    </details>

2. Group the data by skill to compute (i) the total frequency of each skill and (ii) its co-occurrence count with AI terms.
3. Step (1) and (2) are executed by year and then aggregated across all years. Once we have, for each skill $s$, its total appearances and its co-occurrences with AI terms, we compute the AI score $w_{S}^{AI}$. Specifically,  
   
```math
w_{s}^{AI} = \frac{\text{\# jobs with } s \text{ and AI terms}}{\text{\# jobs with } s}

