\section{GeoSynth-SQL Framework}
\label{sec:framework}

\begin{figure*}[t]
\centering
\includegraphics[width=0.97\textwidth]{figures/framework.pdf}
\caption{Overview of the GeoSynth-SQL framework.}
\label{fig:framework}
\end{figure*}

\subsection{Overview}

Figure~\ref{fig:framework} presents our method as a staged synthesis pipeline that progressively transforms raw spatial tables into grounded NL-SQL examples.
The pipeline proceeds step by step, with each stage producing an intermediate artifact for the next. It first canonicalizes heterogeneous source tables into a unified spatial representation, and then organizes them into coherent multi-table spatial databases through relation-driven synthesis. Based on these databases, the framework defines difficulty-controlled spatial SQL skeletons, instantiates them into executable SQL queries, and further converts them into semantically aligned natural language questions. The resulting candidates are finally passed through execution-grounded quality filtering, which combines execution checks with forward and backward LLM-based verification.
Rather than relying on purely manual design, the pipeline integrates LLM-based generation with automated control and filtering mechanisms to improve scalability while maintaining quality and diversity.

The final output is a set of high-quality grounded NL-SQL examples over synthesized multi-table spatial databases, addressing the core challenge of constructing executable and training-ready spatial text-to-SQL data from fragmented open spatial tables.

\subsection{Spatial Table Canonicalization}

To support spatial database synthesis, we first transform raw data sources into a unified canonical form.
Raw data sources collected from the web often exhibit heterogeneous schemas, inconsistent field naming conventions, ambiguous column types, and implicit spatial semantics.
Such heterogeneity makes it difficult to directly identify inter-table relations or synthesize realistic spatial databases.
Therefore, we introduce a spatial table canonicalization stage to normalize table structure, identify spatial columns, and construct a standardized table representation for downstream synthesis.

\subsubsection{Table Normalization}

Given a raw table \(T\) extracted from the web, we first perform table normalization to obtain a cleaner and more explicit schema representation while preserving the heterogeneity commonly observed in real-world open spatial data.
Rather than eliminating schema diversity, this step aims to standardize the surface form of table metadata so that subsequent processing can be carried out in a consistent manner without sacrificing realism.

Specifically, we convert raw field names into database-compatible column names, standardize primitive field types, and identify candidate spatial columns.
This conversion resolves spaces, special symbols, and irregular naming patterns that are common in raw tables, so that the resulting schema can be directly supported by the target database system.
Field types are mapped into a set of column type categories, such as \textit{string}, \textit{number}, \textit{date}, and \textit{spatial}.
At the same time, we preserve sparsity and incomplete attributes whenever possible, since missing values and partially populated columns are common characteristics of real-world open data and should remain reflected in the synthesized setting.
In addition, we explicitly identify spatial columns, including both native geometry fields and coordinate columns that can later be converted into spatial representations.
After normalization, each raw table is converted into a cleaner schema with explicit field names, normalized field types, and identified spatial columns.
This normalized representation provides the basis for subsequent spatial table construction.

\fakeparagraph{Spatial Column Identification}
After schema normalization, we identify columns that carry spatial semantics using a hybrid strategy that combines deterministic rules with LLM-assisted recognition.
In our setting, such columns mainly fall into two categories: geometry columns and coordinate columns.

Geometry columns are relatively easy to detect, as their values are typically encoded in formats such as \texttt{Point}, \texttt{LineString}, \texttt{Polygon}, or \texttt{MultiPolygon}.
Once detected, these columns are directly converted into spatial fields.
Coordinate columns are more diverse.
They may appear either as a single field containing coordinate values or as two separate fields representing longitude and latitude.
To identify them, we use a set of rules based on field names, value patterns, and co-occurrence signals.
When two fields are recognized as a coordinate pair, we merge them into a single spatial field.
Many source datasets are provided in GeoJSON format, which also contains coordinate reference system (CRS) information.
For such cases, we preserve the original CRS recorded in the source data.
If no spatial column can be confidently identified by the rule, we further invoke an LLM with the table schema and sampled values to infer which columns encode spatial semantics.
This hybrid design improves efficiency by resolving easy cases through rules, while retaining robustness on ambiguous schemas through LLM assistance.
The output of this step is an explicit set of spatial fields for each table, which provides the basis for subsequent spatial table construction.

\subsubsection{Canonical Table Representation}

For each raw spatial table \(T_i^{\mathrm{raw}}\), we extract and normalize its metadata, schema, spatial fields, thematic semantics, and representative value samples into a canonical table representation.
This representation provides a unified intermediate abstraction for downstream relation discovery, database synthesis, and query generation.
Specifically, each canonicalized table is represented as
\[
T_i=(t_i,c_i,C_i,m_i,R_i,\Theta_i,\Sigma_i),
\]
where \(t_i\) denotes the table name, \(c_i\) denotes the source city, \(C_i\) denotes the normalized schema, \(m_i\) denotes the semantic summary, \(R_i\) denotes the spatial field list, \(\Theta_i\) denotes the thematic label set, and \(\Sigma_i\) denotes the representative value samples extracted from the table.
The normalized schema is defined as
\[
C_i=\{(a_{ij},\tau_{ij})\}_{j=1}^{|C_i|},
\]
where \(a_{ij}\) is a normalized attribute name and \(\tau_{ij}\) is its normalized data type.
The semantic summary \(m_i\) is generated from the table name, source description, schema information, and representative value samples, and is used to capture the high-level semantics of the table content.

The spatial field list \(R_i\) provides a unified description of the spatial fields contained in \(T_i\).
It is defined as
\[
R_i=\{(f_{ik},g_{ik},s_{ik})\}_{k=1}^{|R_i|},
\]
where \(f_{ik}\) denotes the geometry field name, \(g_{ik}\) denotes the geometry type, and \(s_{ik}\) specifies the coordinate reference information.

\fakeparagraph{Thematic Labeling}
To support relation discovery and database synthesis, we assign standardized thematic labels to each canonicalized table.
Raw open spatial tables are often noisy, heterogeneous, and weakly annotated, making their source categories insufficient for reliable semantic alignment.
Therefore, we map each table to one or more of the 14 global fundamental geospatial data themes defined by UN-GGIM~\cite{unggim2019}.
For each table \(T_i\), the thematic labeling process jointly considers its table name \(t_i\), normalized schema \(C_i\), semantic summary \(m_i\), and representative value samples \(\Sigma_i\).
The resulting label set \(\Theta_i\) provides a normalized semantic abstraction of the table content and is used to guide subsequent relation discovery and multi-table database synthesis.

\subsection{Relation-driven Spatial Database Synthesis}

Given the canonical spatial tables produced in the previous stage, we synthesize multi-table spatial databases by constructing relation-aware table groups within each city.
Instead of randomly combining independent spatial tables, this module first builds a city-specific table relation graph and then samples database schemas from the graph through a controlled random-walk process.
This design preserves semantic coherence among tables while still allowing diverse table combinations that may support spatial joins.

\subsubsection{Relation Graph Construction}
For each city, we first collect all canonical spatial tables belonging to that city.
For each pair of tables \(T_i\) and \(T_j\) in the same city, we compute a semantic similarity score from multiple canonical fields, including table names, semantic summaries, normalized schemas, representative values, and thematic labels.
Specifically, we first encode each field into an embedding vector and sum the embeddings to obtain a table representation.
The similarity score \(s_{ij}\) is then computed between the two table representations.
This score estimates whether two tables are semantically related and thus likely to form a meaningful multi-table spatial database.

Instead of treating the similarity cutoff as an independent hyperparameter, we determine the graph connectivity adaptively according to a target graph density.
Specifically, we rank all table pairs in the same city by their similarity scores and retain the highest-scoring pairs until the resulting graph reaches a target average degree \(\bar{d}\).
For each retained table pair, we add an edge and set its weight as $w_{ij}=s_{ij}$.
The effective similarity cutoff is therefore induced by the retained edge set rather than manually specified.
This construction avoids overly fragmented graphs while preventing unrelated tables from being densely connected.

\subsubsection{Relation-aware Database Sampling}
After constructing the relation graph, we synthesize each database by sampling a table group from the graph.
Inspired by DeepWalk~\cite{Perozzi2014DeepWalk}, we start from a seed table and perform a weighted random walk over the relation graph.
At each step, the next table is selected from the neighbors of the current table with probability proportional to edge weight:
$P(T_j\mid T_i)\propto w_{ij}$.
This transition rule makes the sampler more likely to visit tables that are strongly related to the current table.

Figure~\ref{fig:relation_graph} illustrates an example of this sampling process.
Each node in the relation graph represents a canonical spatial table, and each weighted edge indicates the semantic similarity between two tables.
Starting from a seed table, the sampler follows the numbered trajectory to collect tables into a synthesized database.
For example, the walk first moves from \textit{Road} to \textit{District} and then to \textit{POI} through high-weight edges, which preserves local semantic coherence.
The collected tables are then materialized as a synthesized database, as shown on the right side of the figure.

To improve diversity, we introduce an exploration probability \(\rho\).
At each step, with probability \(1-\rho\), the walk follows the weighted transition over existing graph edges.
With probability \(\rho\), it jumps to a randomly selected table from the same city, regardless of whether the table is directly connected to the current node.
In Figure~\ref{fig:relation_graph}, the dashed arrow denotes such an exploration jump, which allows the sampler to include a table outside the current semantic neighborhood.
In our implementation, we set \(\rho=0.1\), which keeps the sampling process primarily relation-driven while allowing limited exploration beyond the semantic relation graph.
This random jump mechanism is useful because spatial joins can arise from geometric proximity, containment, or intersection even when two tables are not strongly related according to textual or thematic similarity.
Therefore, the sampler can generate databases that are semantically coherent but not restricted to connected table neighborhoods.

\begin{figure}[t]
\centering
\includegraphics[width=0.48\textwidth]{figures/random_walk.pdf}
\caption{An example of a relation-aware database sampling process for a city, where nodes represent tables and edges represent semantic similarity.}
\label{fig:relation_graph}
\end{figure}

\fakeparagraph{Database Size Control}
For each synthesized database, we sample the target number of tables \(K\) from a normal distribution $\mathcal{N}(8,2^2)$.
This design centers the database size around 8 tables while allowing moderate variation across synthesized databases.
The random walk continues until \(K\) distinct tables are collected.
If a visited table has already been included in the current database, the sampler continues walking until a new table is reached or the maximum sampling budget is exceeded.
This size control produces databases with moderate multi-table complexity and keeps the synthesized database scale close to common human-annotated Text-to-SQL benchmarks~\cite{Yu2018Spider,Li2023BIRD,Lei2025Spider2}.

The final sampled table group is materialized as a synthesized spatial database.
Each database preserves the canonical schema, spatial fields, thematic labels, and representative values of its constituent tables, and serves as the basis for subsequent SQL synthesis and natural language question generation.

\subsection{Constraint-guided SQL Synthesis}

After synthesizing relation-aware spatial databases, we generate SQL queries under explicit structural and spatial constraints.
Prior studies on text-to-SQL data synthesis have shown that a SQL-first generation strategy can improve the validity of synthesized samples~\cite{Guo2018QuestionGeneration, Li2025OmniSQL}.
Motivated by this observation, we first synthesize SQL queries and then generate natural language questions conditioned on the generated SQL.
This design is more suitable than directly generating NL-SQL pairs, because SQL queries determine the required tables, joins, predicates, aggregation operators, and spatial functions.
As a result, we can better control the structural difficulty, spatial intent, and execution validity of the generated samples.

\subsubsection{Difficulty Control}

To cover diverse reasoning patterns, we divide synthesized SQL queries into four difficulty levels: Easy, Medium, Hard, and Extra-Hard.
The difficulty design considers both general text-to-SQL complexity and spatial SQL characteristics.
In particular, we consider not only the number of tables and joins, but also the use of spatial predicates, spatial joins, aggregation, nested queries, and multi-step spatial reasoning.
Table~\ref{tab:difficulty} summarizes the difficulty criteria and representative SQL examples.

Easy queries involve a single table and usually contain one spatial predicate, measurement, or transformation.
Medium queries involve two tables and require either a key-based join or a spatial join.
Hard queries involve three or more tables, mixed join types, aggregation, grouping, ordering, or multiple spatial predicates.
Extra-Hard queries further require nested queries, set operations, top-\(k\) spatial reasoning, or multi-stage spatial constraints.
This design prevents the synthesized dataset from being dominated by simple single-table spatial filters, while avoiding uncontrolled generation of overly complex SQL queries.

\begin{table*}[t]
\centering
\caption{Difficulty criteria of \sysname.}
\label{tab:difficulty}
\small
\setlength{\tabcolsep}{4pt}
\begin{tabular}{lccp{10.5cm}}
\toprule
Level & \#Table & \#Join & Representative SQL \\
\midrule
Easy & 1 & None &
\texttt{SELECT name FROM poi WHERE ST\_DWithin(geom, ST\_SetSRID(ST\_Point(-73.9857, 40.7484), 4326), 500);} \\
\midrule
Medium & 2 & Spatial / Key &
\texttt{SELECT s.name FROM school s JOIN district d ON ST\_Within(s.geom, d.geom) WHERE d.name = 'Downtown';} \\
\midrule
Hard & 3+ & Mixed / Multi-step &
\texttt{SELECT d.name, COUNT(*) AS cnt FROM hospital h JOIN district d ON ST\_Within(h.geom, d.geom) JOIN road r ON ST\_DWithin(h.geom, r.geom, 500) WHERE r.type = 'primary' GROUP BY d.name ORDER BY cnt DESC LIMIT 5;} \\
\midrule
Extra-Hard & 3+ & Nested / Multi-stage &
\texttt{SELECT d.name FROM district d WHERE d.geom \&\& (SELECT ST\_Buffer(ST\_Union(p.geom), 1000) FROM park p WHERE p.type = 'public') AND NOT EXISTS (SELECT 1 FROM hospital h WHERE ST\_Within(h.geom, d.geom));} \\
\bottomrule
\end{tabular}
\end{table*}

\subsubsection{Spatial Intent Integration}

Beyond structural difficulty, spatial SQL queries should also cover diverse spatial intents.
In our setting, spatial intents are introduced through PostGIS function constraints.
We parse the PostGIS documentation and construct a spatial function library.
Each function is represented by its function name, input arguments, return type, textual description, and example SQL usages.
For overloaded or polymorphic functions, we preserve different signatures separately, so that the generator can select functions compatible with the schema, geometry type, and target SQL structure.

During SQL synthesis, we first sample a difficulty level and then randomly select one or more spatial functions that are compatible with this level.
For Easy queries, we mainly sample single-table spatial predicates, measurements, or geometry transformations.
For Medium queries, we sample functions that can support spatial joins or two-table spatial predicates.
For Hard and Extra-Hard queries, we allow combinations of spatial predicates, spatial measurements, spatial aggregation, and geometry construction functions.
The selected functions act as explicit spatial intent constraints during SQL generation.
For example, \texttt{ST\_Within} encourages containment reasoning, \texttt{ST\_DWithin} encourages distance-based reasoning, \texttt{ST\_Intersects} encourages intersection reasoning, and \texttt{ST\_Union} or \texttt{ST\_Buffer} can be used to construct multi-stage spatial conditions.
By grounding spatial intent in executable PostGIS functions, the generated SQL queries are more likely to be syntactically valid, spatially meaningful, and executable on the synthesized databases.

\subsubsection{Prompt Design}

The difficulty level controls the structural skeleton of the SQL query, while the sampled PostGIS functions control its spatial intent.
We therefore use a constraint-aware prompt to combine database-specific schema information, difficulty requirements, and spatial function constraints.
Each prompt consists of the following components:

\begin{itemize}[leftmargin=*]
    \item \textbf{Task Goal.}
    The prompt first states the overall objective: generating an executable and spatially meaningful SQL query that satisfies the given constraints.

    \item \textbf{Database Context.}
    We provide the synthesized database schema, including table names, column names, data types, and available table relations.
    We also include spatial field metadata, such as geometry columns, geometry types, and coordinate reference information.

    \item \textbf{Representative Values.}
    Sampled values from selected tables are included to support realistic predicates, filters, and aggregation conditions.

    \item \textbf{Difficulty Constraint.}
    The target difficulty level specifies structural requirements such as the number of tables, join types, aggregation, ordering, and nested queries.

    \item \textbf{Spatial Function Constraint.}
    The sampled spatial functions are provided with their names, input arguments, return types, descriptions, and example usages.

    \item \textbf{Composition Requirements.}
    The prompt specifies how selected tables, joins, predicates, and spatial functions should be combined into a coherent SQL query.

    \item \textbf{Output Format Constraint.}
    The model is required to return the generated SQL in a structured format.
\end{itemize}


\subsection{Diversity-aware NL Question Generation}

After obtaining executable spatial SQL queries, we generate natural language questions conditioned on the SQL semantics.
Unlike direct question generation, the SQL query provides an explicit semantic boundary for the target question, including selected tables, predicates, aggregation conditions, and spatial operations.
Therefore, the generated question should preserve the exact meaning of the SQL while allowing diverse linguistic expressions.
To this end, we introduce two types of diversity control: linguistic style control and spatial relation rephrasing.

\subsubsection{Linguistic Style Control}

A single SQL query can correspond to multiple natural language expressions.
For example, a query that counts schools within a district can be expressed as a direct lookup question, an analytical question, or a user-oriented exploratory question.
If all questions are generated with the same template-like style, the resulting dataset may overfit to limited surface patterns and fail to reflect realistic user queries.
Therefore, we control linguistic diversity by assigning each SQL query a style constraint during question generation.

Specifically, we define several linguistic styles that commonly appear in spatial data analysis scenarios.
These styles include factual lookup, comparative analysis, aggregation-oriented inquiry, ranking-oriented inquiry, and exploratory analysis.
The style constraint determines how the same SQL semantics should be verbalized, while the underlying meaning remains grounded in the SQL query.
For instance, an aggregation SQL can be phrased as ``How many hospitals are located in each district?'' or ``Which districts contain the largest number of hospitals?'' depending on the selected style.
This design increases surface-form diversity without changing the executable semantics of the corresponding SQL.

\subsubsection{Spatial Relation Rephrasing}

Besides general linguistic style, spatial relations require special treatment because the same spatial predicate can be expressed in many natural language forms.
For example, \texttt{ST\_Within} can be verbalized as ``within'', ``inside'', ``located in'', or ``contained by'', while \texttt{ST\_DWithin} can be expressed as ``near'', ``within a distance of'', or ``no farther than''.
Directly using function-name-like expressions would make the generated questions unnatural and reduce linguistic diversity.

We therefore construct a spatial relation rephrasing strategy that maps SQL-level spatial operations to natural language expressions.
During question generation, the prompt provides the selected spatial functions and asks the model to express them using natural language phrases rather than copying function names.
The rephrasing must remain semantically faithful to the SQL predicate.
For example, distance-based predicates should preserve the distance threshold, containment predicates should preserve the direction of containment, and intersection predicates should preserve the intended overlap relation.
In this way, spatial relation rephrasing improves naturalness and diversity while maintaining alignment with the generated SQL.

\subsubsection{Prompt Design}

We use a Diversity-aware prompt to generate natural language questions with controlled diversity.
The prompt includes the SQL query, its associated metadata, and the selected diversity constraints.
Each prompt consists of the following components:

\begin{itemize}[leftmargin=*]
    \item \textbf{Task Goal.}
    The prompt states that the model should generate a natural language question that is semantically equivalent to the given SQL query.

    \item \textbf{SQL.}
    The generated SQL query is provided as the semantic anchor, including its selected tables, predicates, joins, aggregation operators, and spatial functions.

    \item \textbf{Database Context.}
    We provide the relevant table schemas, column names, spatial fields, and representative values to help the model produce realistic entity and attribute descriptions.

    \item \textbf{Linguistic Style Constraint.}
    The selected linguistic style specifies how the question should be expressed, such as factual lookup, comparison, aggregation, ranking, or exploratory analysis.

    \item \textbf{Spatial Relation Constraint.}
    The prompt specifies the spatial predicates appearing in the SQL and requires the model to verbalize them using natural spatial expressions while preserving their exact semantics.

    \item \textbf{Output Format Constraint.}
    The model is required to output the generated question in a structured format.
\end{itemize}

Through this prompt design, each generated question remains grounded in the SQL query while exhibiting diverse linguistic styles and spatial relation expressions.

\subsection{Quality Control}

After SQL and question generation, we apply a quality control process to ensure that each synthesized sample is executable, semantically aligned, and suitable for training.
Since spatial SQL queries involve both relational constraints and geometry-specific constraints, simple syntax checking is insufficient.
Therefore, we combine execution-based validation with self-consistency filtering to remove invalid, duplicated, or semantically inconsistent samples.
The retained samples are used to construct the final spatial text-to-SQL dataset.

\fakeparagraph{Execution Validation}
We first validate each generated SQL query against its corresponding synthesized spatial database.
The validation checks whether the query is syntactically correct, refers only to existing tables and columns, and follows the schema constraints of the database.
For spatial SQL, we further verify geometry type compatibility and spatial function usage, ensuring that the arguments of spatial functions match their expected input types.
We also execute the query and require it to return non-empty results.
Queries that fail any of these checks are removed or sent back for regeneration.

\fakeparagraph{Self-Consistency Filtering}
After execution validation, we perform self-consistency filtering over the generated NL-SQL pairs.
This step removes duplicated samples and filters out questions that are not semantically aligned with their SQL queries.
In particular, the question should preserve all key SQL conditions, including selected tables, predicates, aggregation constraints, ordering requirements, and spatial relations, without introducing unsupported information.
We also balance the retained samples across difficulty levels, spatial functions, and linguistic styles to improve dataset diversity.
Through this filtering process, the final dataset contains executable, diverse, and semantically consistent spatial NL-SQL samples.