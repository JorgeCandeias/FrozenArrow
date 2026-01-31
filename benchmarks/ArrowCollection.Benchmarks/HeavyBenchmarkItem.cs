namespace ArrowCollection.Benchmarks;

/// <summary>
/// Extreme benchmark scenario: A heavy record type with 200 properties (sparse wide dataset).
/// - 10 string properties with low cardinality (100 distinct values each)
/// - 5 DateTime properties for timestamps with high cardinality
/// - 62 int properties (10 randomly active per item, 52 zero - sparse)
/// - 62 double properties (10 randomly active per item, 52 zero - sparse)
/// - 61 decimal properties (10 randomly active per item, 51 zero - sparse)
/// Total: 200 properties
/// 
/// The selection of which 10 numeric properties have values is randomized per item
/// using a fixed seed to simulate realistic sparse data patterns in production use cases.
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
/// Generates sparse data where only 10 properties of each numeric type have values per item,
/// with the active properties randomly selected per item using a fixed seed.
/// </summary>
public static class HeavyBenchmarkItemFactory
{
    private const int StringCardinality = 100;
    private const int ActivePropertiesPerType = 10;
    private const int RandomSeed = 42;
    
    /// <summary>
    /// Generates a list of sparse heavy benchmark items with the specified count.
    /// </summary>
    public static List<HeavyBenchmarkItem> Generate(int count)
    {
        return Generate(count, new Random(RandomSeed));
    }

    /// <summary>
    /// Generates a list of sparse heavy benchmark items with the specified count and random source.
    /// </summary>
    public static List<HeavyBenchmarkItem> Generate(int count, Random random)
    {
        var items = new List<HeavyBenchmarkItem>(count);
        var baseDate = DateTime.UtcNow;
        
        var stringPools = new string[10][];
        for (int pool = 0; pool < 10; pool++)
        {
            stringPools[pool] = Enumerable.Range(0, StringCardinality)
                .Select(i => $"Category{pool:D2}_{i:D3}")
                .ToArray();
        }

        var intIndices = Enumerable.Range(0, 62).ToArray();
        var doubleIndices = Enumerable.Range(0, 62).ToArray();
        var decimalIndices = Enumerable.Range(0, 61).ToArray();

        for (int i = 0; i < count; i++)
        {
            var activeInts = GetRandomIndices(random, intIndices, ActivePropertiesPerType);
            var activeDoubles = GetRandomIndices(random, doubleIndices, ActivePropertiesPerType);
            var activeDecimals = GetRandomIndices(random, decimalIndices, ActivePropertiesPerType);

            var item = new HeavyBenchmarkItem
            {
                // Strings with low cardinality (always populated)
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

                // DateTimes with high cardinality (always populated)
                Timestamp01 = baseDate.AddMilliseconds(-i * 5),
                Timestamp02 = baseDate.AddMilliseconds(-i * 7),
                Timestamp03 = baseDate.AddMilliseconds(-i * 11),
                Timestamp04 = baseDate.AddMilliseconds(-i * 13),
                Timestamp05 = baseDate.AddMilliseconds(-i * 17),
            };

            SetSparseIntValues(item, i, activeInts);
            SetSparseDoubleValues(item, i, activeDoubles);
            SetSparseDecimalValues(item, i, activeDecimals);

            items.Add(item);
        }
        
        return items;
    }

    private static HashSet<int> GetRandomIndices(Random random, int[] allIndices, int count)
    {
        var result = new HashSet<int>();
        var indices = (int[])allIndices.Clone();
        
        for (int i = 0; i < count && i < indices.Length; i++)
        {
            int j = random.Next(i, indices.Length);
            (indices[i], indices[j]) = (indices[j], indices[i]);
            result.Add(indices[i]);
        }
        
        return result;
    }

    private static void SetSparseIntValues(HeavyBenchmarkItem item, int i, HashSet<int> activeIndices)
    {
        if (activeIndices.Contains(0)) item.Int001 = i;
        if (activeIndices.Contains(1)) item.Int002 = i + 1;
        if (activeIndices.Contains(2)) item.Int003 = i + 2;
        if (activeIndices.Contains(3)) item.Int004 = i + 3;
        if (activeIndices.Contains(4)) item.Int005 = i + 4;
        if (activeIndices.Contains(5)) item.Int006 = i + 5;
        if (activeIndices.Contains(6)) item.Int007 = i + 6;
        if (activeIndices.Contains(7)) item.Int008 = i + 7;
        if (activeIndices.Contains(8)) item.Int009 = i + 8;
        if (activeIndices.Contains(9)) item.Int010 = i + 9;
        if (activeIndices.Contains(10)) item.Int011 = i + 10;
        if (activeIndices.Contains(11)) item.Int012 = i + 11;
        if (activeIndices.Contains(12)) item.Int013 = i + 12;
        if (activeIndices.Contains(13)) item.Int014 = i + 13;
        if (activeIndices.Contains(14)) item.Int015 = i + 14;
        if (activeIndices.Contains(15)) item.Int016 = i + 15;
        if (activeIndices.Contains(16)) item.Int017 = i + 16;
        if (activeIndices.Contains(17)) item.Int018 = i + 17;
        if (activeIndices.Contains(18)) item.Int019 = i + 18;
        if (activeIndices.Contains(19)) item.Int020 = i + 19;
        if (activeIndices.Contains(20)) item.Int021 = i + 20;
        if (activeIndices.Contains(21)) item.Int022 = i + 21;
        if (activeIndices.Contains(22)) item.Int023 = i + 22;
        if (activeIndices.Contains(23)) item.Int024 = i + 23;
        if (activeIndices.Contains(24)) item.Int025 = i + 24;
        if (activeIndices.Contains(25)) item.Int026 = i + 25;
        if (activeIndices.Contains(26)) item.Int027 = i + 26;
        if (activeIndices.Contains(27)) item.Int028 = i + 27;
        if (activeIndices.Contains(28)) item.Int029 = i + 28;
        if (activeIndices.Contains(29)) item.Int030 = i + 29;
        if (activeIndices.Contains(30)) item.Int031 = i + 30;
        if (activeIndices.Contains(31)) item.Int032 = i + 31;
        if (activeIndices.Contains(32)) item.Int033 = i + 32;
        if (activeIndices.Contains(33)) item.Int034 = i + 33;
        if (activeIndices.Contains(34)) item.Int035 = i + 34;
        if (activeIndices.Contains(35)) item.Int036 = i + 35;
        if (activeIndices.Contains(36)) item.Int037 = i + 36;
        if (activeIndices.Contains(37)) item.Int038 = i + 37;
        if (activeIndices.Contains(38)) item.Int039 = i + 38;
        if (activeIndices.Contains(39)) item.Int040 = i + 39;
        if (activeIndices.Contains(40)) item.Int041 = i + 40;
        if (activeIndices.Contains(41)) item.Int042 = i + 41;
        if (activeIndices.Contains(42)) item.Int043 = i + 42;
        if (activeIndices.Contains(43)) item.Int044 = i + 43;
        if (activeIndices.Contains(44)) item.Int045 = i + 44;
        if (activeIndices.Contains(45)) item.Int046 = i + 45;
        if (activeIndices.Contains(46)) item.Int047 = i + 46;
        if (activeIndices.Contains(47)) item.Int048 = i + 47;
        if (activeIndices.Contains(48)) item.Int049 = i + 48;
        if (activeIndices.Contains(49)) item.Int050 = i + 49;
        if (activeIndices.Contains(50)) item.Int051 = i + 50;
        if (activeIndices.Contains(51)) item.Int052 = i + 51;
        if (activeIndices.Contains(52)) item.Int053 = i + 52;
        if (activeIndices.Contains(53)) item.Int054 = i + 53;
        if (activeIndices.Contains(54)) item.Int055 = i + 54;
        if (activeIndices.Contains(55)) item.Int056 = i + 55;
        if (activeIndices.Contains(56)) item.Int057 = i + 56;
        if (activeIndices.Contains(57)) item.Int058 = i + 57;
        if (activeIndices.Contains(58)) item.Int059 = i + 58;
        if (activeIndices.Contains(59)) item.Int060 = i + 59;
        if (activeIndices.Contains(60)) item.Int061 = i + 60;
        if (activeIndices.Contains(61)) item.Int062 = i + 61;
    }

    private static void SetSparseDoubleValues(HeavyBenchmarkItem item, int i, HashSet<int> activeIndices)
    {
        if (activeIndices.Contains(0)) item.Double001 = i * 0.001;
        if (activeIndices.Contains(1)) item.Double002 = i * 0.002;
        if (activeIndices.Contains(2)) item.Double003 = i * 0.003;
        if (activeIndices.Contains(3)) item.Double004 = i * 0.004;
        if (activeIndices.Contains(4)) item.Double005 = i * 0.005;
        if (activeIndices.Contains(5)) item.Double006 = i * 0.006;
        if (activeIndices.Contains(6)) item.Double007 = i * 0.007;
        if (activeIndices.Contains(7)) item.Double008 = i * 0.008;
        if (activeIndices.Contains(8)) item.Double009 = i * 0.009;
        if (activeIndices.Contains(9)) item.Double010 = i * 0.010;
        if (activeIndices.Contains(10)) item.Double011 = i * 0.011;
        if (activeIndices.Contains(11)) item.Double012 = i * 0.012;
        if (activeIndices.Contains(12)) item.Double013 = i * 0.013;
        if (activeIndices.Contains(13)) item.Double014 = i * 0.014;
        if (activeIndices.Contains(14)) item.Double015 = i * 0.015;
        if (activeIndices.Contains(15)) item.Double016 = i * 0.016;
        if (activeIndices.Contains(16)) item.Double017 = i * 0.017;
        if (activeIndices.Contains(17)) item.Double018 = i * 0.018;
        if (activeIndices.Contains(18)) item.Double019 = i * 0.019;
        if (activeIndices.Contains(19)) item.Double020 = i * 0.020;
        if (activeIndices.Contains(20)) item.Double021 = i * 0.021;
        if (activeIndices.Contains(21)) item.Double022 = i * 0.022;
        if (activeIndices.Contains(22)) item.Double023 = i * 0.023;
        if (activeIndices.Contains(23)) item.Double024 = i * 0.024;
        if (activeIndices.Contains(24)) item.Double025 = i * 0.025;
        if (activeIndices.Contains(25)) item.Double026 = i * 0.026;
        if (activeIndices.Contains(26)) item.Double027 = i * 0.027;
        if (activeIndices.Contains(27)) item.Double028 = i * 0.028;
        if (activeIndices.Contains(28)) item.Double029 = i * 0.029;
        if (activeIndices.Contains(29)) item.Double030 = i * 0.030;
        if (activeIndices.Contains(30)) item.Double031 = i * 0.031;
        if (activeIndices.Contains(31)) item.Double032 = i * 0.032;
        if (activeIndices.Contains(32)) item.Double033 = i * 0.033;
        if (activeIndices.Contains(33)) item.Double034 = i * 0.034;
        if (activeIndices.Contains(34)) item.Double035 = i * 0.035;
        if (activeIndices.Contains(35)) item.Double036 = i * 0.036;
        if (activeIndices.Contains(36)) item.Double037 = i * 0.037;
        if (activeIndices.Contains(37)) item.Double038 = i * 0.038;
        if (activeIndices.Contains(38)) item.Double039 = i * 0.039;
        if (activeIndices.Contains(39)) item.Double040 = i * 0.040;
        if (activeIndices.Contains(40)) item.Double041 = i * 0.041;
        if (activeIndices.Contains(41)) item.Double042 = i * 0.042;
        if (activeIndices.Contains(42)) item.Double043 = i * 0.043;
        if (activeIndices.Contains(43)) item.Double044 = i * 0.044;
        if (activeIndices.Contains(44)) item.Double045 = i * 0.045;
        if (activeIndices.Contains(45)) item.Double046 = i * 0.046;
        if (activeIndices.Contains(46)) item.Double047 = i * 0.047;
        if (activeIndices.Contains(47)) item.Double048 = i * 0.048;
        if (activeIndices.Contains(48)) item.Double049 = i * 0.049;
        if (activeIndices.Contains(49)) item.Double050 = i * 0.050;
        if (activeIndices.Contains(50)) item.Double051 = i * 0.051;
        if (activeIndices.Contains(51)) item.Double052 = i * 0.052;
        if (activeIndices.Contains(52)) item.Double053 = i * 0.053;
        if (activeIndices.Contains(53)) item.Double054 = i * 0.054;
        if (activeIndices.Contains(54)) item.Double055 = i * 0.055;
        if (activeIndices.Contains(55)) item.Double056 = i * 0.056;
        if (activeIndices.Contains(56)) item.Double057 = i * 0.057;
        if (activeIndices.Contains(57)) item.Double058 = i * 0.058;
        if (activeIndices.Contains(58)) item.Double059 = i * 0.059;
        if (activeIndices.Contains(59)) item.Double060 = i * 0.060;
        if (activeIndices.Contains(60)) item.Double061 = i * 0.061;
        if (activeIndices.Contains(61)) item.Double062 = i * 0.062;
    }

    private static void SetSparseDecimalValues(HeavyBenchmarkItem item, int i, HashSet<int> activeIndices)
    {
        if (activeIndices.Contains(0)) item.Decimal001 = i * 0.001m;
        if (activeIndices.Contains(1)) item.Decimal002 = i * 0.002m;
        if (activeIndices.Contains(2)) item.Decimal003 = i * 0.003m;
        if (activeIndices.Contains(3)) item.Decimal004 = i * 0.004m;
        if (activeIndices.Contains(4)) item.Decimal005 = i * 0.005m;
        if (activeIndices.Contains(5)) item.Decimal006 = i * 0.006m;
        if (activeIndices.Contains(6)) item.Decimal007 = i * 0.007m;
        if (activeIndices.Contains(7)) item.Decimal008 = i * 0.008m;
        if (activeIndices.Contains(8)) item.Decimal009 = i * 0.009m;
        if (activeIndices.Contains(9)) item.Decimal010 = i * 0.010m;
        if (activeIndices.Contains(10)) item.Decimal011 = i * 0.011m;
        if (activeIndices.Contains(11)) item.Decimal012 = i * 0.012m;
        if (activeIndices.Contains(12)) item.Decimal013 = i * 0.013m;
        if (activeIndices.Contains(13)) item.Decimal014 = i * 0.014m;
        if (activeIndices.Contains(14)) item.Decimal015 = i * 0.015m;
        if (activeIndices.Contains(15)) item.Decimal016 = i * 0.016m;
        if (activeIndices.Contains(16)) item.Decimal017 = i * 0.017m;
        if (activeIndices.Contains(17)) item.Decimal018 = i * 0.018m;
        if (activeIndices.Contains(18)) item.Decimal019 = i * 0.019m;
        if (activeIndices.Contains(19)) item.Decimal020 = i * 0.020m;
        if (activeIndices.Contains(20)) item.Decimal021 = i * 0.021m;
        if (activeIndices.Contains(21)) item.Decimal022 = i * 0.022m;
        if (activeIndices.Contains(22)) item.Decimal023 = i * 0.023m;
        if (activeIndices.Contains(23)) item.Decimal024 = i * 0.024m;
        if (activeIndices.Contains(24)) item.Decimal025 = i * 0.025m;
        if (activeIndices.Contains(25)) item.Decimal026 = i * 0.026m;
        if (activeIndices.Contains(26)) item.Decimal027 = i * 0.027m;
        if (activeIndices.Contains(27)) item.Decimal028 = i * 0.028m;
        if (activeIndices.Contains(28)) item.Decimal029 = i * 0.029m;
        if (activeIndices.Contains(29)) item.Decimal030 = i * 0.030m;
        if (activeIndices.Contains(30)) item.Decimal031 = i * 0.031m;
        if (activeIndices.Contains(31)) item.Decimal032 = i * 0.032m;
        if (activeIndices.Contains(32)) item.Decimal033 = i * 0.033m;
        if (activeIndices.Contains(33)) item.Decimal034 = i * 0.034m;
        if (activeIndices.Contains(34)) item.Decimal035 = i * 0.035m;
        if (activeIndices.Contains(35)) item.Decimal036 = i * 0.036m;
        if (activeIndices.Contains(36)) item.Decimal037 = i * 0.037m;
        if (activeIndices.Contains(37)) item.Decimal038 = i * 0.038m;
        if (activeIndices.Contains(38)) item.Decimal039 = i * 0.039m;
        if (activeIndices.Contains(39)) item.Decimal040 = i * 0.040m;
        if (activeIndices.Contains(40)) item.Decimal041 = i * 0.041m;
        if (activeIndices.Contains(41)) item.Decimal042 = i * 0.042m;
        if (activeIndices.Contains(42)) item.Decimal043 = i * 0.043m;
        if (activeIndices.Contains(43)) item.Decimal044 = i * 0.044m;
        if (activeIndices.Contains(44)) item.Decimal045 = i * 0.045m;
        if (activeIndices.Contains(45)) item.Decimal046 = i * 0.046m;
        if (activeIndices.Contains(46)) item.Decimal047 = i * 0.047m;
        if (activeIndices.Contains(47)) item.Decimal048 = i * 0.048m;
        if (activeIndices.Contains(48)) item.Decimal049 = i * 0.049m;
        if (activeIndices.Contains(49)) item.Decimal050 = i * 0.050m;
        if (activeIndices.Contains(50)) item.Decimal051 = i * 0.051m;
        if (activeIndices.Contains(51)) item.Decimal052 = i * 0.052m;
        if (activeIndices.Contains(52)) item.Decimal053 = i * 0.053m;
        if (activeIndices.Contains(53)) item.Decimal054 = i * 0.054m;
        if (activeIndices.Contains(54)) item.Decimal055 = i * 0.055m;
        if (activeIndices.Contains(55)) item.Decimal056 = i * 0.056m;
        if (activeIndices.Contains(56)) item.Decimal057 = i * 0.057m;
        if (activeIndices.Contains(57)) item.Decimal058 = i * 0.058m;
        if (activeIndices.Contains(58)) item.Decimal059 = i * 0.059m;
        if (activeIndices.Contains(59)) item.Decimal060 = i * 0.060m;
        if (activeIndices.Contains(60)) item.Decimal061 = i * 0.061m;
    }
}
