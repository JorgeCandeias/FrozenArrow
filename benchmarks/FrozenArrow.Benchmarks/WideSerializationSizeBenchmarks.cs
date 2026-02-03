using Apache.Arrow.Ipc;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Order;
using ProtoBuf;

namespace FrozenArrow.Benchmarks;

/// <summary>
/// Benchmarks comparing serialization size for wide data types (200 columns) with 1 million items.
/// Compares Arrow IPC (with various compression options) against Protobuf.
/// </summary>
[MemoryDiagnoser]
[Orderer(SummaryOrderPolicy.FastestToSlowest)]
[ShortRunJob]
public class WideSerializationSizeBenchmarks
{
    private List<HeavyBenchmarkItem> _items = null!;
    private FrozenArrow<HeavyBenchmarkItem> _frozenArrow = null!;
    private ProtobufHeavyCollection _protobufCollection = null!;

    private ArrowWriteOptions _noCompression = null!;
    private ArrowWriteOptions _lz4Compression = null!;
    private ArrowWriteOptions _zstdCompression = null!;

    [Params(10_000, 100_000, 1_000_000)]
    public int ItemCount { get; set; }

    [GlobalSetup]
    public void Setup()
    {
        _items = HeavyBenchmarkItemFactory.Generate(ItemCount);
        _frozenArrow = _items.ToFrozenArrow();
        _protobufCollection = new ProtobufHeavyCollection 
        { 
            Items = _items.Select(ProtobufHeavyItem.FromHeavyBenchmarkItem).ToList() 
        };

        _noCompression = new ArrowWriteOptions { CompressionCodec = null };
        _lz4Compression = new ArrowWriteOptions { CompressionCodec = CompressionCodecType.Lz4Frame };
        _zstdCompression = new ArrowWriteOptions { CompressionCodec = CompressionCodecType.Zstd };
    }

    [GlobalCleanup]
    public void Cleanup()
    {
        _frozenArrow?.Dispose();
    }

    #region Arrow Benchmarks

    [Benchmark]
    [BenchmarkCategory("WideData_Serialization")]
    public long Arrow_NoCompression()
    {
        using var stream = new MemoryStream();
        _frozenArrow.WriteToAsync(stream, _noCompression).GetAwaiter().GetResult();
        return stream.Length;
    }

    [Benchmark]
    [BenchmarkCategory("WideData_Serialization")]
    public long Arrow_Lz4Compression()
    {
        using var stream = new MemoryStream();
        _frozenArrow.WriteToAsync(stream, _lz4Compression).GetAwaiter().GetResult();
        return stream.Length;
    }

    [Benchmark]
    [BenchmarkCategory("WideData_Serialization")]
    public long Arrow_ZstdCompression()
    {
        using var stream = new MemoryStream();
        _frozenArrow.WriteToAsync(stream, _zstdCompression).GetAwaiter().GetResult();
        return stream.Length;
    }

    #endregion

    #region Protobuf Benchmarks

    [Benchmark]
    [BenchmarkCategory("WideData_Serialization")]
    public long Protobuf()
    {
        using var stream = new MemoryStream();
        Serializer.Serialize(stream, _protobufCollection);
        return stream.Length;
    }

    #endregion
}

/// <summary>
/// Protobuf-compatible version of HeavyBenchmarkItem with 200 columns.
/// Mirrors the structure of HeavyBenchmarkItem for fair comparison.
/// </summary>
[ProtoContract]
public class ProtobufHeavyItem
{
    #region String Properties (10 total)
    
    [ProtoMember(1)] public string String01 { get; set; } = "";
    [ProtoMember(2)] public string String02 { get; set; } = "";
    [ProtoMember(3)] public string String03 { get; set; } = "";
    [ProtoMember(4)] public string String04 { get; set; } = "";
    [ProtoMember(5)] public string String05 { get; set; } = "";
    [ProtoMember(6)] public string String06 { get; set; } = "";
    [ProtoMember(7)] public string String07 { get; set; } = "";
    [ProtoMember(8)] public string String08 { get; set; } = "";
    [ProtoMember(9)] public string String09 { get; set; } = "";
    [ProtoMember(10)] public string String10 { get; set; } = "";
    
    #endregion

    #region DateTime Properties (5 total)
    
    [ProtoMember(11)] public DateTime Timestamp01 { get; set; }
    [ProtoMember(12)] public DateTime Timestamp02 { get; set; }
    [ProtoMember(13)] public DateTime Timestamp03 { get; set; }
    [ProtoMember(14)] public DateTime Timestamp04 { get; set; }
    [ProtoMember(15)] public DateTime Timestamp05 { get; set; }
    
    #endregion

    #region Int Properties (62 total)
    
    [ProtoMember(16)] public int Int001 { get; set; }
    [ProtoMember(17)] public int Int002 { get; set; }
    [ProtoMember(18)] public int Int003 { get; set; }
    [ProtoMember(19)] public int Int004 { get; set; }
    [ProtoMember(20)] public int Int005 { get; set; }
    [ProtoMember(21)] public int Int006 { get; set; }
    [ProtoMember(22)] public int Int007 { get; set; }
    [ProtoMember(23)] public int Int008 { get; set; }
    [ProtoMember(24)] public int Int009 { get; set; }
    [ProtoMember(25)] public int Int010 { get; set; }
    [ProtoMember(26)] public int Int011 { get; set; }
    [ProtoMember(27)] public int Int012 { get; set; }
    [ProtoMember(28)] public int Int013 { get; set; }
    [ProtoMember(29)] public int Int014 { get; set; }
    [ProtoMember(30)] public int Int015 { get; set; }
    [ProtoMember(31)] public int Int016 { get; set; }
    [ProtoMember(32)] public int Int017 { get; set; }
    [ProtoMember(33)] public int Int018 { get; set; }
    [ProtoMember(34)] public int Int019 { get; set; }
    [ProtoMember(35)] public int Int020 { get; set; }
    [ProtoMember(36)] public int Int021 { get; set; }
    [ProtoMember(37)] public int Int022 { get; set; }
    [ProtoMember(38)] public int Int023 { get; set; }
    [ProtoMember(39)] public int Int024 { get; set; }
    [ProtoMember(40)] public int Int025 { get; set; }
    [ProtoMember(41)] public int Int026 { get; set; }
    [ProtoMember(42)] public int Int027 { get; set; }
    [ProtoMember(43)] public int Int028 { get; set; }
    [ProtoMember(44)] public int Int029 { get; set; }
    [ProtoMember(45)] public int Int030 { get; set; }
    [ProtoMember(46)] public int Int031 { get; set; }
    [ProtoMember(47)] public int Int032 { get; set; }
    [ProtoMember(48)] public int Int033 { get; set; }
    [ProtoMember(49)] public int Int034 { get; set; }
    [ProtoMember(50)] public int Int035 { get; set; }
    [ProtoMember(51)] public int Int036 { get; set; }
    [ProtoMember(52)] public int Int037 { get; set; }
    [ProtoMember(53)] public int Int038 { get; set; }
    [ProtoMember(54)] public int Int039 { get; set; }
    [ProtoMember(55)] public int Int040 { get; set; }
    [ProtoMember(56)] public int Int041 { get; set; }
    [ProtoMember(57)] public int Int042 { get; set; }
    [ProtoMember(58)] public int Int043 { get; set; }
    [ProtoMember(59)] public int Int044 { get; set; }
    [ProtoMember(60)] public int Int045 { get; set; }
    [ProtoMember(61)] public int Int046 { get; set; }
    [ProtoMember(62)] public int Int047 { get; set; }
    [ProtoMember(63)] public int Int048 { get; set; }
    [ProtoMember(64)] public int Int049 { get; set; }
    [ProtoMember(65)] public int Int050 { get; set; }
    [ProtoMember(66)] public int Int051 { get; set; }
    [ProtoMember(67)] public int Int052 { get; set; }
    [ProtoMember(68)] public int Int053 { get; set; }
    [ProtoMember(69)] public int Int054 { get; set; }
    [ProtoMember(70)] public int Int055 { get; set; }
    [ProtoMember(71)] public int Int056 { get; set; }
    [ProtoMember(72)] public int Int057 { get; set; }
    [ProtoMember(73)] public int Int058 { get; set; }
    [ProtoMember(74)] public int Int059 { get; set; }
    [ProtoMember(75)] public int Int060 { get; set; }
    [ProtoMember(76)] public int Int061 { get; set; }
    [ProtoMember(77)] public int Int062 { get; set; }
    
    #endregion

    #region Double Properties (62 total)
    
    [ProtoMember(78)] public double Double001 { get; set; }
    [ProtoMember(79)] public double Double002 { get; set; }
    [ProtoMember(80)] public double Double003 { get; set; }
    [ProtoMember(81)] public double Double004 { get; set; }
    [ProtoMember(82)] public double Double005 { get; set; }
    [ProtoMember(83)] public double Double006 { get; set; }
    [ProtoMember(84)] public double Double007 { get; set; }
    [ProtoMember(85)] public double Double008 { get; set; }
    [ProtoMember(86)] public double Double009 { get; set; }
    [ProtoMember(87)] public double Double010 { get; set; }
    [ProtoMember(88)] public double Double011 { get; set; }
    [ProtoMember(89)] public double Double012 { get; set; }
    [ProtoMember(90)] public double Double013 { get; set; }
    [ProtoMember(91)] public double Double014 { get; set; }
    [ProtoMember(92)] public double Double015 { get; set; }
    [ProtoMember(93)] public double Double016 { get; set; }
    [ProtoMember(94)] public double Double017 { get; set; }
    [ProtoMember(95)] public double Double018 { get; set; }
    [ProtoMember(96)] public double Double019 { get; set; }
    [ProtoMember(97)] public double Double020 { get; set; }
    [ProtoMember(98)] public double Double021 { get; set; }
    [ProtoMember(99)] public double Double022 { get; set; }
    [ProtoMember(100)] public double Double023 { get; set; }
    [ProtoMember(101)] public double Double024 { get; set; }
    [ProtoMember(102)] public double Double025 { get; set; }
    [ProtoMember(103)] public double Double026 { get; set; }
    [ProtoMember(104)] public double Double027 { get; set; }
    [ProtoMember(105)] public double Double028 { get; set; }
    [ProtoMember(106)] public double Double029 { get; set; }
    [ProtoMember(107)] public double Double030 { get; set; }
    [ProtoMember(108)] public double Double031 { get; set; }
    [ProtoMember(109)] public double Double032 { get; set; }
    [ProtoMember(110)] public double Double033 { get; set; }
    [ProtoMember(111)] public double Double034 { get; set; }
    [ProtoMember(112)] public double Double035 { get; set; }
    [ProtoMember(113)] public double Double036 { get; set; }
    [ProtoMember(114)] public double Double037 { get; set; }
    [ProtoMember(115)] public double Double038 { get; set; }
    [ProtoMember(116)] public double Double039 { get; set; }
    [ProtoMember(117)] public double Double040 { get; set; }
    [ProtoMember(118)] public double Double041 { get; set; }
    [ProtoMember(119)] public double Double042 { get; set; }
    [ProtoMember(120)] public double Double043 { get; set; }
    [ProtoMember(121)] public double Double044 { get; set; }
    [ProtoMember(122)] public double Double045 { get; set; }
    [ProtoMember(123)] public double Double046 { get; set; }
    [ProtoMember(124)] public double Double047 { get; set; }
    [ProtoMember(125)] public double Double048 { get; set; }
    [ProtoMember(126)] public double Double049 { get; set; }
    [ProtoMember(127)] public double Double050 { get; set; }
    [ProtoMember(128)] public double Double051 { get; set; }
    [ProtoMember(129)] public double Double052 { get; set; }
    [ProtoMember(130)] public double Double053 { get; set; }
    [ProtoMember(131)] public double Double054 { get; set; }
    [ProtoMember(132)] public double Double055 { get; set; }
    [ProtoMember(133)] public double Double056 { get; set; }
    [ProtoMember(134)] public double Double057 { get; set; }
    [ProtoMember(135)] public double Double058 { get; set; }
    [ProtoMember(136)] public double Double059 { get; set; }
    [ProtoMember(137)] public double Double060 { get; set; }
    [ProtoMember(138)] public double Double061 { get; set; }
    [ProtoMember(139)] public double Double062 { get; set; }
    
    #endregion

    #region Decimal Properties (61 total)
    
    [ProtoMember(140)] public decimal Decimal001 { get; set; }
    [ProtoMember(141)] public decimal Decimal002 { get; set; }
    [ProtoMember(142)] public decimal Decimal003 { get; set; }
    [ProtoMember(143)] public decimal Decimal004 { get; set; }
    [ProtoMember(144)] public decimal Decimal005 { get; set; }
    [ProtoMember(145)] public decimal Decimal006 { get; set; }
    [ProtoMember(146)] public decimal Decimal007 { get; set; }
    [ProtoMember(147)] public decimal Decimal008 { get; set; }
    [ProtoMember(148)] public decimal Decimal009 { get; set; }
    [ProtoMember(149)] public decimal Decimal010 { get; set; }
    [ProtoMember(150)] public decimal Decimal011 { get; set; }
    [ProtoMember(151)] public decimal Decimal012 { get; set; }
    [ProtoMember(152)] public decimal Decimal013 { get; set; }
    [ProtoMember(153)] public decimal Decimal014 { get; set; }
    [ProtoMember(154)] public decimal Decimal015 { get; set; }
    [ProtoMember(155)] public decimal Decimal016 { get; set; }
    [ProtoMember(156)] public decimal Decimal017 { get; set; }
    [ProtoMember(157)] public decimal Decimal018 { get; set; }
    [ProtoMember(158)] public decimal Decimal019 { get; set; }
    [ProtoMember(159)] public decimal Decimal020 { get; set; }
    [ProtoMember(160)] public decimal Decimal021 { get; set; }
    [ProtoMember(161)] public decimal Decimal022 { get; set; }
    [ProtoMember(162)] public decimal Decimal023 { get; set; }
    [ProtoMember(163)] public decimal Decimal024 { get; set; }
    [ProtoMember(164)] public decimal Decimal025 { get; set; }
    [ProtoMember(165)] public decimal Decimal026 { get; set; }
    [ProtoMember(166)] public decimal Decimal027 { get; set; }
    [ProtoMember(167)] public decimal Decimal028 { get; set; }
    [ProtoMember(168)] public decimal Decimal029 { get; set; }
    [ProtoMember(169)] public decimal Decimal030 { get; set; }
    [ProtoMember(170)] public decimal Decimal031 { get; set; }
    [ProtoMember(171)] public decimal Decimal032 { get; set; }
    [ProtoMember(172)] public decimal Decimal033 { get; set; }
    [ProtoMember(173)] public decimal Decimal034 { get; set; }
    [ProtoMember(174)] public decimal Decimal035 { get; set; }
    [ProtoMember(175)] public decimal Decimal036 { get; set; }
    [ProtoMember(176)] public decimal Decimal037 { get; set; }
    [ProtoMember(177)] public decimal Decimal038 { get; set; }
    [ProtoMember(178)] public decimal Decimal039 { get; set; }
    [ProtoMember(179)] public decimal Decimal040 { get; set; }
    [ProtoMember(180)] public decimal Decimal041 { get; set; }
    [ProtoMember(181)] public decimal Decimal042 { get; set; }
    [ProtoMember(182)] public decimal Decimal043 { get; set; }
    [ProtoMember(183)] public decimal Decimal044 { get; set; }
    [ProtoMember(184)] public decimal Decimal045 { get; set; }
    [ProtoMember(185)] public decimal Decimal046 { get; set; }
    [ProtoMember(186)] public decimal Decimal047 { get; set; }
    [ProtoMember(187)] public decimal Decimal048 { get; set; }
    [ProtoMember(188)] public decimal Decimal049 { get; set; }
    [ProtoMember(189)] public decimal Decimal050 { get; set; }
    [ProtoMember(190)] public decimal Decimal051 { get; set; }
    [ProtoMember(191)] public decimal Decimal052 { get; set; }
    [ProtoMember(192)] public decimal Decimal053 { get; set; }
    [ProtoMember(193)] public decimal Decimal054 { get; set; }
    [ProtoMember(194)] public decimal Decimal055 { get; set; }
    [ProtoMember(195)] public decimal Decimal056 { get; set; }
    [ProtoMember(196)] public decimal Decimal057 { get; set; }
    [ProtoMember(197)] public decimal Decimal058 { get; set; }
    [ProtoMember(198)] public decimal Decimal059 { get; set; }
    [ProtoMember(199)] public decimal Decimal060 { get; set; }
    [ProtoMember(200)] public decimal Decimal061 { get; set; }
    
    #endregion

    /// <summary>
    /// Creates a ProtobufHeavyItem from a HeavyBenchmarkItem.
    /// </summary>
    public static ProtobufHeavyItem FromHeavyBenchmarkItem(HeavyBenchmarkItem source) => new()
    {
        String01 = source.String01,
        String02 = source.String02,
        String03 = source.String03,
        String04 = source.String04,
        String05 = source.String05,
        String06 = source.String06,
        String07 = source.String07,
        String08 = source.String08,
        String09 = source.String09,
        String10 = source.String10,
        Timestamp01 = source.Timestamp01,
        Timestamp02 = source.Timestamp02,
        Timestamp03 = source.Timestamp03,
        Timestamp04 = source.Timestamp04,
        Timestamp05 = source.Timestamp05,
        Int001 = source.Int001,
        Int002 = source.Int002,
        Int003 = source.Int003,
        Int004 = source.Int004,
        Int005 = source.Int005,
        Int006 = source.Int006,
        Int007 = source.Int007,
        Int008 = source.Int008,
        Int009 = source.Int009,
        Int010 = source.Int010,
        Int011 = source.Int011,
        Int012 = source.Int012,
        Int013 = source.Int013,
        Int014 = source.Int014,
        Int015 = source.Int015,
        Int016 = source.Int016,
        Int017 = source.Int017,
        Int018 = source.Int018,
        Int019 = source.Int019,
        Int020 = source.Int020,
        Int021 = source.Int021,
        Int022 = source.Int022,
        Int023 = source.Int023,
        Int024 = source.Int024,
        Int025 = source.Int025,
        Int026 = source.Int026,
        Int027 = source.Int027,
        Int028 = source.Int028,
        Int029 = source.Int029,
        Int030 = source.Int030,
        Int031 = source.Int031,
        Int032 = source.Int032,
        Int033 = source.Int033,
        Int034 = source.Int034,
        Int035 = source.Int035,
        Int036 = source.Int036,
        Int037 = source.Int037,
        Int038 = source.Int038,
        Int039 = source.Int039,
        Int040 = source.Int040,
        Int041 = source.Int041,
        Int042 = source.Int042,
        Int043 = source.Int043,
        Int044 = source.Int044,
        Int045 = source.Int045,
        Int046 = source.Int046,
        Int047 = source.Int047,
        Int048 = source.Int048,
        Int049 = source.Int049,
        Int050 = source.Int050,
        Int051 = source.Int051,
        Int052 = source.Int052,
        Int053 = source.Int053,
        Int054 = source.Int054,
        Int055 = source.Int055,
        Int056 = source.Int056,
        Int057 = source.Int057,
        Int058 = source.Int058,
        Int059 = source.Int059,
        Int060 = source.Int060,
        Int061 = source.Int061,
        Int062 = source.Int062,
        Double001 = source.Double001,
        Double002 = source.Double002,
        Double003 = source.Double003,
        Double004 = source.Double004,
        Double005 = source.Double005,
        Double006 = source.Double006,
        Double007 = source.Double007,
        Double008 = source.Double008,
        Double009 = source.Double009,
        Double010 = source.Double010,
        Double011 = source.Double011,
        Double012 = source.Double012,
        Double013 = source.Double013,
        Double014 = source.Double014,
        Double015 = source.Double015,
        Double016 = source.Double016,
        Double017 = source.Double017,
        Double018 = source.Double018,
        Double019 = source.Double019,
        Double020 = source.Double020,
        Double021 = source.Double021,
        Double022 = source.Double022,
        Double023 = source.Double023,
        Double024 = source.Double024,
        Double025 = source.Double025,
        Double026 = source.Double026,
        Double027 = source.Double027,
        Double028 = source.Double028,
        Double029 = source.Double029,
        Double030 = source.Double030,
        Double031 = source.Double031,
        Double032 = source.Double032,
        Double033 = source.Double033,
        Double034 = source.Double034,
        Double035 = source.Double035,
        Double036 = source.Double036,
        Double037 = source.Double037,
        Double038 = source.Double038,
        Double039 = source.Double039,
        Double040 = source.Double040,
        Double041 = source.Double041,
        Double042 = source.Double042,
        Double043 = source.Double043,
        Double044 = source.Double044,
        Double045 = source.Double045,
        Double046 = source.Double046,
        Double047 = source.Double047,
        Double048 = source.Double048,
        Double049 = source.Double049,
        Double050 = source.Double050,
        Double051 = source.Double051,
        Double052 = source.Double052,
        Double053 = source.Double053,
        Double054 = source.Double054,
        Double055 = source.Double055,
        Double056 = source.Double056,
        Double057 = source.Double057,
        Double058 = source.Double058,
        Double059 = source.Double059,
        Double060 = source.Double060,
        Double061 = source.Double061,
        Double062 = source.Double062,
        Decimal001 = source.Decimal001,
        Decimal002 = source.Decimal002,
        Decimal003 = source.Decimal003,
        Decimal004 = source.Decimal004,
        Decimal005 = source.Decimal005,
        Decimal006 = source.Decimal006,
        Decimal007 = source.Decimal007,
        Decimal008 = source.Decimal008,
        Decimal009 = source.Decimal009,
        Decimal010 = source.Decimal010,
        Decimal011 = source.Decimal011,
        Decimal012 = source.Decimal012,
        Decimal013 = source.Decimal013,
        Decimal014 = source.Decimal014,
        Decimal015 = source.Decimal015,
        Decimal016 = source.Decimal016,
        Decimal017 = source.Decimal017,
        Decimal018 = source.Decimal018,
        Decimal019 = source.Decimal019,
        Decimal020 = source.Decimal020,
        Decimal021 = source.Decimal021,
        Decimal022 = source.Decimal022,
        Decimal023 = source.Decimal023,
        Decimal024 = source.Decimal024,
        Decimal025 = source.Decimal025,
        Decimal026 = source.Decimal026,
        Decimal027 = source.Decimal027,
        Decimal028 = source.Decimal028,
        Decimal029 = source.Decimal029,
        Decimal030 = source.Decimal030,
        Decimal031 = source.Decimal031,
        Decimal032 = source.Decimal032,
        Decimal033 = source.Decimal033,
        Decimal034 = source.Decimal034,
        Decimal035 = source.Decimal035,
        Decimal036 = source.Decimal036,
        Decimal037 = source.Decimal037,
        Decimal038 = source.Decimal038,
        Decimal039 = source.Decimal039,
        Decimal040 = source.Decimal040,
        Decimal041 = source.Decimal041,
        Decimal042 = source.Decimal042,
        Decimal043 = source.Decimal043,
        Decimal044 = source.Decimal044,
        Decimal045 = source.Decimal045,
        Decimal046 = source.Decimal046,
        Decimal047 = source.Decimal047,
        Decimal048 = source.Decimal048,
        Decimal049 = source.Decimal049,
        Decimal050 = source.Decimal050,
        Decimal051 = source.Decimal051,
        Decimal052 = source.Decimal052,
        Decimal053 = source.Decimal053,
        Decimal054 = source.Decimal054,
        Decimal055 = source.Decimal055,
        Decimal056 = source.Decimal056,
        Decimal057 = source.Decimal057,
        Decimal058 = source.Decimal058,
        Decimal059 = source.Decimal059,
        Decimal060 = source.Decimal060,
        Decimal061 = source.Decimal061,
    };
}

/// <summary>
/// Wrapper class for Protobuf serialization of the heavy item collection.
/// </summary>
[ProtoContract]
public class ProtobufHeavyCollection
{
    [ProtoMember(1)]
    public List<ProtobufHeavyItem> Items { get; set; } = [];
}
