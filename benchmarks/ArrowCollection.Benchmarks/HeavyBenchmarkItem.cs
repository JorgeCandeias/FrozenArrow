namespace ArrowCollection.Benchmarks;

/// <summary>
/// Extreme benchmark scenario: A heavy record type with 200 properties.
/// - 10 string properties with low cardinality (100 distinct values each)
/// - 5 DateTime properties for timestamps with high cardinality
/// - 62 int properties with high cardinality
/// - 62 double properties with high cardinality  
/// - 61 decimal properties with high cardinality
/// Total: 200 properties
/// </summary>
[ArrowRecord]
public class HeavyBenchmarkItem
{
    #region String Properties (10 total - low cardinality, 100 distinct values each)
    
    [ArrowArray] public string String01 { get; set; } = "";
    [ArrowArray] public string String02 { get; set; } = "";
    [ArrowArray] public string String03 { get; set; } = "";
    [ArrowArray] public string String04 { get; set; } = "";
    [ArrowArray] public string String05 { get; set; } = "";
    [ArrowArray] public string String06 { get; set; } = "";
    [ArrowArray] public string String07 { get; set; } = "";
    [ArrowArray] public string String08 { get; set; } = "";
    [ArrowArray] public string String09 { get; set; } = "";
    [ArrowArray] public string String10 { get; set; } = "";
    
    #endregion

    #region DateTime Properties (5 total - high cardinality timestamps)
    
    [ArrowArray] public DateTime Timestamp01 { get; set; }
    [ArrowArray] public DateTime Timestamp02 { get; set; }
    [ArrowArray] public DateTime Timestamp03 { get; set; }
    [ArrowArray] public DateTime Timestamp04 { get; set; }
    [ArrowArray] public DateTime Timestamp05 { get; set; }
    
    #endregion

    #region Int Properties (62 total - high cardinality)
    
    [ArrowArray] public int Int001 { get; set; }
    [ArrowArray] public int Int002 { get; set; }
    [ArrowArray] public int Int003 { get; set; }
    [ArrowArray] public int Int004 { get; set; }
    [ArrowArray] public int Int005 { get; set; }
    [ArrowArray] public int Int006 { get; set; }
    [ArrowArray] public int Int007 { get; set; }
    [ArrowArray] public int Int008 { get; set; }
    [ArrowArray] public int Int009 { get; set; }
    [ArrowArray] public int Int010 { get; set; }
    [ArrowArray] public int Int011 { get; set; }
    [ArrowArray] public int Int012 { get; set; }
    [ArrowArray] public int Int013 { get; set; }
    [ArrowArray] public int Int014 { get; set; }
    [ArrowArray] public int Int015 { get; set; }
    [ArrowArray] public int Int016 { get; set; }
    [ArrowArray] public int Int017 { get; set; }
    [ArrowArray] public int Int018 { get; set; }
    [ArrowArray] public int Int019 { get; set; }
    [ArrowArray] public int Int020 { get; set; }
    [ArrowArray] public int Int021 { get; set; }
    [ArrowArray] public int Int022 { get; set; }
    [ArrowArray] public int Int023 { get; set; }
    [ArrowArray] public int Int024 { get; set; }
    [ArrowArray] public int Int025 { get; set; }
    [ArrowArray] public int Int026 { get; set; }
    [ArrowArray] public int Int027 { get; set; }
    [ArrowArray] public int Int028 { get; set; }
    [ArrowArray] public int Int029 { get; set; }
    [ArrowArray] public int Int030 { get; set; }
    [ArrowArray] public int Int031 { get; set; }
    [ArrowArray] public int Int032 { get; set; }
    [ArrowArray] public int Int033 { get; set; }
    [ArrowArray] public int Int034 { get; set; }
    [ArrowArray] public int Int035 { get; set; }
    [ArrowArray] public int Int036 { get; set; }
    [ArrowArray] public int Int037 { get; set; }
    [ArrowArray] public int Int038 { get; set; }
    [ArrowArray] public int Int039 { get; set; }
    [ArrowArray] public int Int040 { get; set; }
    [ArrowArray] public int Int041 { get; set; }
    [ArrowArray] public int Int042 { get; set; }
    [ArrowArray] public int Int043 { get; set; }
    [ArrowArray] public int Int044 { get; set; }
    [ArrowArray] public int Int045 { get; set; }
    [ArrowArray] public int Int046 { get; set; }
    [ArrowArray] public int Int047 { get; set; }
    [ArrowArray] public int Int048 { get; set; }
    [ArrowArray] public int Int049 { get; set; }
    [ArrowArray] public int Int050 { get; set; }
    [ArrowArray] public int Int051 { get; set; }
    [ArrowArray] public int Int052 { get; set; }
    [ArrowArray] public int Int053 { get; set; }
    [ArrowArray] public int Int054 { get; set; }
    [ArrowArray] public int Int055 { get; set; }
    [ArrowArray] public int Int056 { get; set; }
    [ArrowArray] public int Int057 { get; set; }
    [ArrowArray] public int Int058 { get; set; }
    [ArrowArray] public int Int059 { get; set; }
    [ArrowArray] public int Int060 { get; set; }
    [ArrowArray] public int Int061 { get; set; }
    [ArrowArray] public int Int062 { get; set; }
    
    #endregion

    #region Double Properties (62 total - high cardinality)
    
    [ArrowArray] public double Double001 { get; set; }
    [ArrowArray] public double Double002 { get; set; }
    [ArrowArray] public double Double003 { get; set; }
    [ArrowArray] public double Double004 { get; set; }
    [ArrowArray] public double Double005 { get; set; }
    [ArrowArray] public double Double006 { get; set; }
    [ArrowArray] public double Double007 { get; set; }
    [ArrowArray] public double Double008 { get; set; }
    [ArrowArray] public double Double009 { get; set; }
    [ArrowArray] public double Double010 { get; set; }
    [ArrowArray] public double Double011 { get; set; }
    [ArrowArray] public double Double012 { get; set; }
    [ArrowArray] public double Double013 { get; set; }
    [ArrowArray] public double Double014 { get; set; }
    [ArrowArray] public double Double015 { get; set; }
    [ArrowArray] public double Double016 { get; set; }
    [ArrowArray] public double Double017 { get; set; }
    [ArrowArray] public double Double018 { get; set; }
    [ArrowArray] public double Double019 { get; set; }
    [ArrowArray] public double Double020 { get; set; }
    [ArrowArray] public double Double021 { get; set; }
    [ArrowArray] public double Double022 { get; set; }
    [ArrowArray] public double Double023 { get; set; }
    [ArrowArray] public double Double024 { get; set; }
    [ArrowArray] public double Double025 { get; set; }
    [ArrowArray] public double Double026 { get; set; }
    [ArrowArray] public double Double027 { get; set; }
    [ArrowArray] public double Double028 { get; set; }
    [ArrowArray] public double Double029 { get; set; }
    [ArrowArray] public double Double030 { get; set; }
    [ArrowArray] public double Double031 { get; set; }
    [ArrowArray] public double Double032 { get; set; }
    [ArrowArray] public double Double033 { get; set; }
    [ArrowArray] public double Double034 { get; set; }
    [ArrowArray] public double Double035 { get; set; }
    [ArrowArray] public double Double036 { get; set; }
    [ArrowArray] public double Double037 { get; set; }
    [ArrowArray] public double Double038 { get; set; }
    [ArrowArray] public double Double039 { get; set; }
    [ArrowArray] public double Double040 { get; set; }
    [ArrowArray] public double Double041 { get; set; }
    [ArrowArray] public double Double042 { get; set; }
    [ArrowArray] public double Double043 { get; set; }
    [ArrowArray] public double Double044 { get; set; }
    [ArrowArray] public double Double045 { get; set; }
    [ArrowArray] public double Double046 { get; set; }
    [ArrowArray] public double Double047 { get; set; }
    [ArrowArray] public double Double048 { get; set; }
    [ArrowArray] public double Double049 { get; set; }
    [ArrowArray] public double Double050 { get; set; }
    [ArrowArray] public double Double051 { get; set; }
    [ArrowArray] public double Double052 { get; set; }
    [ArrowArray] public double Double053 { get; set; }
    [ArrowArray] public double Double054 { get; set; }
    [ArrowArray] public double Double055 { get; set; }
    [ArrowArray] public double Double056 { get; set; }
    [ArrowArray] public double Double057 { get; set; }
    [ArrowArray] public double Double058 { get; set; }
    [ArrowArray] public double Double059 { get; set; }
    [ArrowArray] public double Double060 { get; set; }
    [ArrowArray] public double Double061 { get; set; }
    [ArrowArray] public double Double062 { get; set; }
    
    #endregion

    #region Decimal Properties (61 total - high cardinality)
    
    [ArrowArray] public decimal Decimal001 { get; set; }
    [ArrowArray] public decimal Decimal002 { get; set; }
    [ArrowArray] public decimal Decimal003 { get; set; }
    [ArrowArray] public decimal Decimal004 { get; set; }
    [ArrowArray] public decimal Decimal005 { get; set; }
    [ArrowArray] public decimal Decimal006 { get; set; }
    [ArrowArray] public decimal Decimal007 { get; set; }
    [ArrowArray] public decimal Decimal008 { get; set; }
    [ArrowArray] public decimal Decimal009 { get; set; }
    [ArrowArray] public decimal Decimal010 { get; set; }
    [ArrowArray] public decimal Decimal011 { get; set; }
    [ArrowArray] public decimal Decimal012 { get; set; }
    [ArrowArray] public decimal Decimal013 { get; set; }
    [ArrowArray] public decimal Decimal014 { get; set; }
    [ArrowArray] public decimal Decimal015 { get; set; }
    [ArrowArray] public decimal Decimal016 { get; set; }
    [ArrowArray] public decimal Decimal017 { get; set; }
    [ArrowArray] public decimal Decimal018 { get; set; }
    [ArrowArray] public decimal Decimal019 { get; set; }
    [ArrowArray] public decimal Decimal020 { get; set; }
    [ArrowArray] public decimal Decimal021 { get; set; }
    [ArrowArray] public decimal Decimal022 { get; set; }
    [ArrowArray] public decimal Decimal023 { get; set; }
    [ArrowArray] public decimal Decimal024 { get; set; }
    [ArrowArray] public decimal Decimal025 { get; set; }
    [ArrowArray] public decimal Decimal026 { get; set; }
    [ArrowArray] public decimal Decimal027 { get; set; }
    [ArrowArray] public decimal Decimal028 { get; set; }
    [ArrowArray] public decimal Decimal029 { get; set; }
    [ArrowArray] public decimal Decimal030 { get; set; }
    [ArrowArray] public decimal Decimal031 { get; set; }
    [ArrowArray] public decimal Decimal032 { get; set; }
    [ArrowArray] public decimal Decimal033 { get; set; }
    [ArrowArray] public decimal Decimal034 { get; set; }
    [ArrowArray] public decimal Decimal035 { get; set; }
    [ArrowArray] public decimal Decimal036 { get; set; }
    [ArrowArray] public decimal Decimal037 { get; set; }
    [ArrowArray] public decimal Decimal038 { get; set; }
    [ArrowArray] public decimal Decimal039 { get; set; }
    [ArrowArray] public decimal Decimal040 { get; set; }
    [ArrowArray] public decimal Decimal041 { get; set; }
    [ArrowArray] public decimal Decimal042 { get; set; }
    [ArrowArray] public decimal Decimal043 { get; set; }
    [ArrowArray] public decimal Decimal044 { get; set; }
    [ArrowArray] public decimal Decimal045 { get; set; }
    [ArrowArray] public decimal Decimal046 { get; set; }
    [ArrowArray] public decimal Decimal047 { get; set; }
    [ArrowArray] public decimal Decimal048 { get; set; }
    [ArrowArray] public decimal Decimal049 { get; set; }
    [ArrowArray] public decimal Decimal050 { get; set; }
    [ArrowArray] public decimal Decimal051 { get; set; }
    [ArrowArray] public decimal Decimal052 { get; set; }
    [ArrowArray] public decimal Decimal053 { get; set; }
    [ArrowArray] public decimal Decimal054 { get; set; }
    [ArrowArray] public decimal Decimal055 { get; set; }
    [ArrowArray] public decimal Decimal056 { get; set; }
    [ArrowArray] public decimal Decimal057 { get; set; }
    [ArrowArray] public decimal Decimal058 { get; set; }
    [ArrowArray] public decimal Decimal059 { get; set; }
    [ArrowArray] public decimal Decimal060 { get; set; }
    [ArrowArray] public decimal Decimal061 { get; set; }
    
    #endregion
}

/// <summary>
/// Factory to generate HeavyBenchmarkItem instances for benchmarking.
/// </summary>
public static class HeavyBenchmarkItemFactory
{
    private const int StringCardinality = 100;
    
    /// <summary>
    /// Generates a list of heavy benchmark items with the specified count.
    /// </summary>
    public static List<HeavyBenchmarkItem> Generate(int count)
    {
        var items = new List<HeavyBenchmarkItem>(count);
        var baseDate = DateTime.UtcNow;
        var random = new Random(42); // Fixed seed for reproducibility
        
        // Pre-generate string pools for low cardinality (100 distinct values each)
        var stringPools = new string[10][];
        for (int pool = 0; pool < 10; pool++)
        {
            stringPools[pool] = Enumerable.Range(0, StringCardinality)
                .Select(i => $"Category{pool:D2}_{i:D3}")
                .ToArray();
        }

        for (int i = 0; i < count; i++)
        {
            var item = new HeavyBenchmarkItem
            {
                // String properties - low cardinality (cycle through 100 distinct values)
                String01 = stringPools[0][i % StringCardinality],
                String02 = stringPools[1][i % StringCardinality],
                String03 = stringPools[2][i % StringCardinality],
                String04 = stringPools[3][i % StringCardinality],
                String05 = stringPools[4][i % StringCardinality],
                String06 = stringPools[5][i % StringCardinality],
                String07 = stringPools[6][i % StringCardinality],
                String08 = stringPools[7][i % StringCardinality],
                String09 = stringPools[8][i % StringCardinality],
                String10 = stringPools[9][i % StringCardinality],

                // DateTime properties - high cardinality (unique timestamps)
                Timestamp01 = baseDate.AddMilliseconds(-i * 5),
                Timestamp02 = baseDate.AddMilliseconds(-i * 7),
                Timestamp03 = baseDate.AddMilliseconds(-i * 11),
                Timestamp04 = baseDate.AddMilliseconds(-i * 13),
                Timestamp05 = baseDate.AddMilliseconds(-i * 17),

                // Int properties - high cardinality
                Int001 = i,
                Int002 = i + 1,
                Int003 = i + 2,
                Int004 = i + 3,
                Int005 = i + 4,
                Int006 = i + 5,
                Int007 = i + 6,
                Int008 = i + 7,
                Int009 = i + 8,
                Int010 = i + 9,
                Int011 = i + 10,
                Int012 = i + 11,
                Int013 = i + 12,
                Int014 = i + 13,
                Int015 = i + 14,
                Int016 = i + 15,
                Int017 = i + 16,
                Int018 = i + 17,
                Int019 = i + 18,
                Int020 = i + 19,
                Int021 = i + 20,
                Int022 = i + 21,
                Int023 = i + 22,
                Int024 = i + 23,
                Int025 = i + 24,
                Int026 = i + 25,
                Int027 = i + 26,
                Int028 = i + 27,
                Int029 = i + 28,
                Int030 = i + 29,
                Int031 = i + 30,
                Int032 = i + 31,
                Int033 = i + 32,
                Int034 = i + 33,
                Int035 = i + 34,
                Int036 = i + 35,
                Int037 = i + 36,
                Int038 = i + 37,
                Int039 = i + 38,
                Int040 = i + 39,
                Int041 = i + 40,
                Int042 = i + 41,
                Int043 = i + 42,
                Int044 = i + 43,
                Int045 = i + 44,
                Int046 = i + 45,
                Int047 = i + 46,
                Int048 = i + 47,
                Int049 = i + 48,
                Int050 = i + 49,
                Int051 = i + 50,
                Int052 = i + 51,
                Int053 = i + 52,
                Int054 = i + 53,
                Int055 = i + 54,
                Int056 = i + 55,
                Int057 = i + 56,
                Int058 = i + 57,
                Int059 = i + 58,
                Int060 = i + 59,
                Int061 = i + 60,
                Int062 = i + 61,

                // Double properties - high cardinality
                Double001 = i * 0.001,
                Double002 = i * 0.002,
                Double003 = i * 0.003,
                Double004 = i * 0.004,
                Double005 = i * 0.005,
                Double006 = i * 0.006,
                Double007 = i * 0.007,
                Double008 = i * 0.008,
                Double009 = i * 0.009,
                Double010 = i * 0.010,
                Double011 = i * 0.011,
                Double012 = i * 0.012,
                Double013 = i * 0.013,
                Double014 = i * 0.014,
                Double015 = i * 0.015,
                Double016 = i * 0.016,
                Double017 = i * 0.017,
                Double018 = i * 0.018,
                Double019 = i * 0.019,
                Double020 = i * 0.020,
                Double021 = i * 0.021,
                Double022 = i * 0.022,
                Double023 = i * 0.023,
                Double024 = i * 0.024,
                Double025 = i * 0.025,
                Double026 = i * 0.026,
                Double027 = i * 0.027,
                Double028 = i * 0.028,
                Double029 = i * 0.029,
                Double030 = i * 0.030,
                Double031 = i * 0.031,
                Double032 = i * 0.032,
                Double033 = i * 0.033,
                Double034 = i * 0.034,
                Double035 = i * 0.035,
                Double036 = i * 0.036,
                Double037 = i * 0.037,
                Double038 = i * 0.038,
                Double039 = i * 0.039,
                Double040 = i * 0.040,
                Double041 = i * 0.041,
                Double042 = i * 0.042,
                Double043 = i * 0.043,
                Double044 = i * 0.044,
                Double045 = i * 0.045,
                Double046 = i * 0.046,
                Double047 = i * 0.047,
                Double048 = i * 0.048,
                Double049 = i * 0.049,
                Double050 = i * 0.050,
                Double051 = i * 0.051,
                Double052 = i * 0.052,
                Double053 = i * 0.053,
                Double054 = i * 0.054,
                Double055 = i * 0.055,
                Double056 = i * 0.056,
                Double057 = i * 0.057,
                Double058 = i * 0.058,
                Double059 = i * 0.059,
                Double060 = i * 0.060,
                Double061 = i * 0.061,
                Double062 = i * 0.062,

                // Decimal properties - high cardinality
                Decimal001 = i * 0.001m,
                Decimal002 = i * 0.002m,
                Decimal003 = i * 0.003m,
                Decimal004 = i * 0.004m,
                Decimal005 = i * 0.005m,
                Decimal006 = i * 0.006m,
                Decimal007 = i * 0.007m,
                Decimal008 = i * 0.008m,
                Decimal009 = i * 0.009m,
                Decimal010 = i * 0.010m,
                Decimal011 = i * 0.011m,
                Decimal012 = i * 0.012m,
                Decimal013 = i * 0.013m,
                Decimal014 = i * 0.014m,
                Decimal015 = i * 0.015m,
                Decimal016 = i * 0.016m,
                Decimal017 = i * 0.017m,
                Decimal018 = i * 0.018m,
                Decimal019 = i * 0.019m,
                Decimal020 = i * 0.020m,
                Decimal021 = i * 0.021m,
                Decimal022 = i * 0.022m,
                Decimal023 = i * 0.023m,
                Decimal024 = i * 0.024m,
                Decimal025 = i * 0.025m,
                Decimal026 = i * 0.026m,
                Decimal027 = i * 0.027m,
                Decimal028 = i * 0.028m,
                Decimal029 = i * 0.029m,
                Decimal030 = i * 0.030m,
                Decimal031 = i * 0.031m,
                Decimal032 = i * 0.032m,
                Decimal033 = i * 0.033m,
                Decimal034 = i * 0.034m,
                Decimal035 = i * 0.035m,
                Decimal036 = i * 0.036m,
                Decimal037 = i * 0.037m,
                Decimal038 = i * 0.038m,
                Decimal039 = i * 0.039m,
                Decimal040 = i * 0.040m,
                Decimal041 = i * 0.041m,
                Decimal042 = i * 0.042m,
                Decimal043 = i * 0.043m,
                Decimal044 = i * 0.044m,
                Decimal045 = i * 0.045m,
                Decimal046 = i * 0.046m,
                Decimal047 = i * 0.047m,
                Decimal048 = i * 0.048m,
                Decimal049 = i * 0.049m,
                Decimal050 = i * 0.050m,
                Decimal051 = i * 0.051m,
                Decimal052 = i * 0.052m,
                Decimal053 = i * 0.053m,
                Decimal054 = i * 0.054m,
                Decimal055 = i * 0.055m,
                Decimal056 = i * 0.056m,
                Decimal057 = i * 0.057m,
                Decimal058 = i * 0.058m,
                Decimal059 = i * 0.059m,
                Decimal060 = i * 0.060m,
                Decimal061 = i * 0.061m
            };
            
            items.Add(item);
        }
        
        return items;
    }
}
