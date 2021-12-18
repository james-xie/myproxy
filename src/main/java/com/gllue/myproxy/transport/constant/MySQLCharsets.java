package com.gllue.myproxy.transport.constant;

import java.nio.charset.Charset;
import lombok.Getter;

/**
 * The mysql supported charsets and collations.
 *
 * <pre>
 * @see <a href="https://dev.mysql.com/doc/internals/en/character-set.html#packet-Protocol::CharacterSet">CharacterSet</a>
 * </pre>
 */
public final class MySQLCharsets {
  public static final String UTF8MB4 = "utf8mb4";
  public static final String UTF8 = "utf8";
  public static final int UTF8MB4_CHARSET_ID = 45;


  private static final MySQLCharset[] CHARSETS = new MySQLCharset[310];

  /*
   * Charsets is fetched from the mysql database (version: 8.0.25) by following sql:
   *
   * SELECT ID, CHARACTER_SET_NAME, COLLATION_NAME, IS_DEFAULT FROM INFORMATION_SCHEMA.COLLATIONS ORDER BY ID;
   */
  static {
    CHARSETS[1] = new MySQLCharset(1, "big5", "big5_chinese_ci", true);
    CHARSETS[2] = new MySQLCharset(2, "latin2", "latin2_czech_cs", false);
    CHARSETS[3] = new MySQLCharset(3, "dec8", "dec8_swedish_ci", true);
    CHARSETS[4] = new MySQLCharset(4, "cp850", "cp850_general_ci", true);
    CHARSETS[5] = new MySQLCharset(5, "latin1", "latin1_german1_ci", false);
    CHARSETS[6] = new MySQLCharset(6, "hp8", "hp8_english_ci", true);
    CHARSETS[7] = new MySQLCharset(7, "koi8r", "koi8r_general_ci", true);
    CHARSETS[8] = new MySQLCharset(8, "latin1", "latin1_swedish_ci", true);
    CHARSETS[9] = new MySQLCharset(9, "latin2", "latin2_general_ci", true);
    CHARSETS[10] = new MySQLCharset(10, "swe7", "swe7_swedish_ci", true);
    CHARSETS[11] = new MySQLCharset(11, "ascii", "ascii_general_ci", true);
    CHARSETS[12] = new MySQLCharset(12, "ujis", "ujis_japanese_ci", true);
    CHARSETS[13] = new MySQLCharset(13, "sjis", "sjis_japanese_ci", true);
    CHARSETS[14] = new MySQLCharset(14, "cp1251", "cp1251_bulgarian_ci", false);
    CHARSETS[15] = new MySQLCharset(15, "latin1", "latin1_danish_ci", false);
    CHARSETS[16] = new MySQLCharset(16, "hebrew", "hebrew_general_ci", true);
    CHARSETS[18] = new MySQLCharset(18, "tis620", "tis620_thai_ci", true);
    CHARSETS[19] = new MySQLCharset(19, "euckr", "euckr_korean_ci", true);
    CHARSETS[20] = new MySQLCharset(20, "latin7", "latin7_estonian_cs", false);
    CHARSETS[21] = new MySQLCharset(21, "latin2", "latin2_hungarian_ci", false);
    CHARSETS[22] = new MySQLCharset(22, "koi8u", "koi8u_general_ci", true);
    CHARSETS[23] = new MySQLCharset(23, "cp1251", "cp1251_ukrainian_ci", false);
    CHARSETS[24] = new MySQLCharset(24, "gb2312", "gb2312_chinese_ci", true);
    CHARSETS[25] = new MySQLCharset(25, "greek", "greek_general_ci", true);
    CHARSETS[26] = new MySQLCharset(26, "cp1250", "cp1250_general_ci", true);
    CHARSETS[27] = new MySQLCharset(27, "latin2", "latin2_croatian_ci", false);
    CHARSETS[28] = new MySQLCharset(28, "gbk", "gbk_chinese_ci", true);
    CHARSETS[29] = new MySQLCharset(29, "cp1257", "cp1257_lithuanian_ci", false);
    CHARSETS[30] = new MySQLCharset(30, "latin5", "latin5_turkish_ci", true);
    CHARSETS[31] = new MySQLCharset(31, "latin1", "latin1_german2_ci", false);
    CHARSETS[32] = new MySQLCharset(32, "armscii8", "armscii8_general_ci", true);
    CHARSETS[33] = new MySQLCharset(33, "utf8", "utf8_general_ci", true);
    CHARSETS[34] = new MySQLCharset(34, "cp1250", "cp1250_czech_cs", false);
    CHARSETS[35] = new MySQLCharset(35, "ucs2", "ucs2_general_ci", true);
    CHARSETS[36] = new MySQLCharset(36, "cp866", "cp866_general_ci", true);
    CHARSETS[37] = new MySQLCharset(37, "keybcs2", "keybcs2_general_ci", true);
    CHARSETS[38] = new MySQLCharset(38, "macce", "macce_general_ci", true);
    CHARSETS[39] = new MySQLCharset(39, "macroman", "macroman_general_ci", true);
    CHARSETS[40] = new MySQLCharset(40, "cp852", "cp852_general_ci", true);
    CHARSETS[41] = new MySQLCharset(41, "latin7", "latin7_general_ci", true);
    CHARSETS[42] = new MySQLCharset(42, "latin7", "latin7_general_cs", false);
    CHARSETS[43] = new MySQLCharset(43, "macce", "macce_bin", false);
    CHARSETS[44] = new MySQLCharset(44, "cp1250", "cp1250_croatian_ci", false);
    CHARSETS[45] = new MySQLCharset(45, "utf8mb4", "utf8mb4_general_ci", false);
    CHARSETS[46] = new MySQLCharset(46, "utf8mb4", "utf8mb4_bin", false);
    CHARSETS[47] = new MySQLCharset(47, "latin1", "latin1_bin", false);
    CHARSETS[48] = new MySQLCharset(48, "latin1", "latin1_general_ci", false);
    CHARSETS[49] = new MySQLCharset(49, "latin1", "latin1_general_cs", false);
    CHARSETS[50] = new MySQLCharset(50, "cp1251", "cp1251_bin", false);
    CHARSETS[51] = new MySQLCharset(51, "cp1251", "cp1251_general_ci", true);
    CHARSETS[52] = new MySQLCharset(52, "cp1251", "cp1251_general_cs", false);
    CHARSETS[53] = new MySQLCharset(53, "macroman", "macroman_bin", false);
    CHARSETS[54] = new MySQLCharset(54, "utf16", "utf16_general_ci", true);
    CHARSETS[55] = new MySQLCharset(55, "utf16", "utf16_bin", false);
    CHARSETS[56] = new MySQLCharset(56, "utf16le", "utf16le_general_ci", true);
    CHARSETS[57] = new MySQLCharset(57, "cp1256", "cp1256_general_ci", true);
    CHARSETS[58] = new MySQLCharset(58, "cp1257", "cp1257_bin", false);
    CHARSETS[59] = new MySQLCharset(59, "cp1257", "cp1257_general_ci", true);
    CHARSETS[60] = new MySQLCharset(60, "utf32", "utf32_general_ci", true);
    CHARSETS[61] = new MySQLCharset(61, "utf32", "utf32_bin", false);
    CHARSETS[62] = new MySQLCharset(62, "utf16le", "utf16le_bin", false);
    CHARSETS[63] = new MySQLCharset(63, "binary", "binary", true);
    CHARSETS[64] = new MySQLCharset(64, "armscii8", "armscii8_bin", false);
    CHARSETS[65] = new MySQLCharset(65, "ascii", "ascii_bin", false);
    CHARSETS[66] = new MySQLCharset(66, "cp1250", "cp1250_bin", false);
    CHARSETS[67] = new MySQLCharset(67, "cp1256", "cp1256_bin", false);
    CHARSETS[68] = new MySQLCharset(68, "cp866", "cp866_bin", false);
    CHARSETS[69] = new MySQLCharset(69, "dec8", "dec8_bin", false);
    CHARSETS[70] = new MySQLCharset(70, "greek", "greek_bin", false);
    CHARSETS[71] = new MySQLCharset(71, "hebrew", "hebrew_bin", false);
    CHARSETS[72] = new MySQLCharset(72, "hp8", "hp8_bin", false);
    CHARSETS[73] = new MySQLCharset(73, "keybcs2", "keybcs2_bin", false);
    CHARSETS[74] = new MySQLCharset(74, "koi8r", "koi8r_bin", false);
    CHARSETS[75] = new MySQLCharset(75, "koi8u", "koi8u_bin", false);
    CHARSETS[76] = new MySQLCharset(76, "utf8", "utf8_tolower_ci", false);
    CHARSETS[77] = new MySQLCharset(77, "latin2", "latin2_bin", false);
    CHARSETS[78] = new MySQLCharset(78, "latin5", "latin5_bin", false);
    CHARSETS[79] = new MySQLCharset(79, "latin7", "latin7_bin", false);
    CHARSETS[80] = new MySQLCharset(80, "cp850", "cp850_bin", false);
    CHARSETS[81] = new MySQLCharset(81, "cp852", "cp852_bin", false);
    CHARSETS[82] = new MySQLCharset(82, "swe7", "swe7_bin", false);
    CHARSETS[83] = new MySQLCharset(83, "utf8", "utf8_bin", false);
    CHARSETS[84] = new MySQLCharset(84, "big5", "big5_bin", false);
    CHARSETS[85] = new MySQLCharset(85, "euckr", "euckr_bin", false);
    CHARSETS[86] = new MySQLCharset(86, "gb2312", "gb2312_bin", false);
    CHARSETS[87] = new MySQLCharset(87, "gbk", "gbk_bin", false);
    CHARSETS[88] = new MySQLCharset(88, "sjis", "sjis_bin", false);
    CHARSETS[89] = new MySQLCharset(89, "tis620", "tis620_bin", false);
    CHARSETS[90] = new MySQLCharset(90, "ucs2", "ucs2_bin", false);
    CHARSETS[91] = new MySQLCharset(91, "ujis", "ujis_bin", false);
    CHARSETS[92] = new MySQLCharset(92, "geostd8", "geostd8_general_ci", true);
    CHARSETS[93] = new MySQLCharset(93, "geostd8", "geostd8_bin", false);
    CHARSETS[94] = new MySQLCharset(94, "latin1", "latin1_spanish_ci", false);
    CHARSETS[95] = new MySQLCharset(95, "cp932", "cp932_japanese_ci", true);
    CHARSETS[96] = new MySQLCharset(96, "cp932", "cp932_bin", false);
    CHARSETS[97] = new MySQLCharset(97, "eucjpms", "eucjpms_japanese_ci", true);
    CHARSETS[98] = new MySQLCharset(98, "eucjpms", "eucjpms_bin", false);
    CHARSETS[99] = new MySQLCharset(99, "cp1250", "cp1250_polish_ci", false);
    CHARSETS[101] = new MySQLCharset(101, "utf16", "utf16_unicode_ci", false);
    CHARSETS[102] = new MySQLCharset(102, "utf16", "utf16_icelandic_ci", false);
    CHARSETS[103] = new MySQLCharset(103, "utf16", "utf16_latvian_ci", false);
    CHARSETS[104] = new MySQLCharset(104, "utf16", "utf16_romanian_ci", false);
    CHARSETS[105] = new MySQLCharset(105, "utf16", "utf16_slovenian_ci", false);
    CHARSETS[106] = new MySQLCharset(106, "utf16", "utf16_polish_ci", false);
    CHARSETS[107] = new MySQLCharset(107, "utf16", "utf16_estonian_ci", false);
    CHARSETS[108] = new MySQLCharset(108, "utf16", "utf16_spanish_ci", false);
    CHARSETS[109] = new MySQLCharset(109, "utf16", "utf16_swedish_ci", false);
    CHARSETS[110] = new MySQLCharset(110, "utf16", "utf16_turkish_ci", false);
    CHARSETS[111] = new MySQLCharset(111, "utf16", "utf16_czech_ci", false);
    CHARSETS[112] = new MySQLCharset(112, "utf16", "utf16_danish_ci", false);
    CHARSETS[113] = new MySQLCharset(113, "utf16", "utf16_lithuanian_ci", false);
    CHARSETS[114] = new MySQLCharset(114, "utf16", "utf16_slovak_ci", false);
    CHARSETS[115] = new MySQLCharset(115, "utf16", "utf16_spanish2_ci", false);
    CHARSETS[116] = new MySQLCharset(116, "utf16", "utf16_roman_ci", false);
    CHARSETS[117] = new MySQLCharset(117, "utf16", "utf16_persian_ci", false);
    CHARSETS[118] = new MySQLCharset(118, "utf16", "utf16_esperanto_ci", false);
    CHARSETS[119] = new MySQLCharset(119, "utf16", "utf16_hungarian_ci", false);
    CHARSETS[120] = new MySQLCharset(120, "utf16", "utf16_sinhala_ci", false);
    CHARSETS[121] = new MySQLCharset(121, "utf16", "utf16_german2_ci", false);
    CHARSETS[122] = new MySQLCharset(122, "utf16", "utf16_croatian_ci", false);
    CHARSETS[123] = new MySQLCharset(123, "utf16", "utf16_unicode_520_ci", false);
    CHARSETS[124] = new MySQLCharset(124, "utf16", "utf16_vietnamese_ci", false);
    CHARSETS[128] = new MySQLCharset(128, "ucs2", "ucs2_unicode_ci", false);
    CHARSETS[129] = new MySQLCharset(129, "ucs2", "ucs2_icelandic_ci", false);
    CHARSETS[130] = new MySQLCharset(130, "ucs2", "ucs2_latvian_ci", false);
    CHARSETS[131] = new MySQLCharset(131, "ucs2", "ucs2_romanian_ci", false);
    CHARSETS[132] = new MySQLCharset(132, "ucs2", "ucs2_slovenian_ci", false);
    CHARSETS[133] = new MySQLCharset(133, "ucs2", "ucs2_polish_ci", false);
    CHARSETS[134] = new MySQLCharset(134, "ucs2", "ucs2_estonian_ci", false);
    CHARSETS[135] = new MySQLCharset(135, "ucs2", "ucs2_spanish_ci", false);
    CHARSETS[136] = new MySQLCharset(136, "ucs2", "ucs2_swedish_ci", false);
    CHARSETS[137] = new MySQLCharset(137, "ucs2", "ucs2_turkish_ci", false);
    CHARSETS[138] = new MySQLCharset(138, "ucs2", "ucs2_czech_ci", false);
    CHARSETS[139] = new MySQLCharset(139, "ucs2", "ucs2_danish_ci", false);
    CHARSETS[140] = new MySQLCharset(140, "ucs2", "ucs2_lithuanian_ci", false);
    CHARSETS[141] = new MySQLCharset(141, "ucs2", "ucs2_slovak_ci", false);
    CHARSETS[142] = new MySQLCharset(142, "ucs2", "ucs2_spanish2_ci", false);
    CHARSETS[143] = new MySQLCharset(143, "ucs2", "ucs2_roman_ci", false);
    CHARSETS[144] = new MySQLCharset(144, "ucs2", "ucs2_persian_ci", false);
    CHARSETS[145] = new MySQLCharset(145, "ucs2", "ucs2_esperanto_ci", false);
    CHARSETS[146] = new MySQLCharset(146, "ucs2", "ucs2_hungarian_ci", false);
    CHARSETS[147] = new MySQLCharset(147, "ucs2", "ucs2_sinhala_ci", false);
    CHARSETS[148] = new MySQLCharset(148, "ucs2", "ucs2_german2_ci", false);
    CHARSETS[149] = new MySQLCharset(149, "ucs2", "ucs2_croatian_ci", false);
    CHARSETS[150] = new MySQLCharset(150, "ucs2", "ucs2_unicode_520_ci", false);
    CHARSETS[151] = new MySQLCharset(151, "ucs2", "ucs2_vietnamese_ci", false);
    CHARSETS[159] = new MySQLCharset(159, "ucs2", "ucs2_general_mysql500_ci", false);
    CHARSETS[160] = new MySQLCharset(160, "utf32", "utf32_unicode_ci", false);
    CHARSETS[161] = new MySQLCharset(161, "utf32", "utf32_icelandic_ci", false);
    CHARSETS[162] = new MySQLCharset(162, "utf32", "utf32_latvian_ci", false);
    CHARSETS[163] = new MySQLCharset(163, "utf32", "utf32_romanian_ci", false);
    CHARSETS[164] = new MySQLCharset(164, "utf32", "utf32_slovenian_ci", false);
    CHARSETS[165] = new MySQLCharset(165, "utf32", "utf32_polish_ci", false);
    CHARSETS[166] = new MySQLCharset(166, "utf32", "utf32_estonian_ci", false);
    CHARSETS[167] = new MySQLCharset(167, "utf32", "utf32_spanish_ci", false);
    CHARSETS[168] = new MySQLCharset(168, "utf32", "utf32_swedish_ci", false);
    CHARSETS[169] = new MySQLCharset(169, "utf32", "utf32_turkish_ci", false);
    CHARSETS[170] = new MySQLCharset(170, "utf32", "utf32_czech_ci", false);
    CHARSETS[171] = new MySQLCharset(171, "utf32", "utf32_danish_ci", false);
    CHARSETS[172] = new MySQLCharset(172, "utf32", "utf32_lithuanian_ci", false);
    CHARSETS[173] = new MySQLCharset(173, "utf32", "utf32_slovak_ci", false);
    CHARSETS[174] = new MySQLCharset(174, "utf32", "utf32_spanish2_ci", false);
    CHARSETS[175] = new MySQLCharset(175, "utf32", "utf32_roman_ci", false);
    CHARSETS[176] = new MySQLCharset(176, "utf32", "utf32_persian_ci", false);
    CHARSETS[177] = new MySQLCharset(177, "utf32", "utf32_esperanto_ci", false);
    CHARSETS[178] = new MySQLCharset(178, "utf32", "utf32_hungarian_ci", false);
    CHARSETS[179] = new MySQLCharset(179, "utf32", "utf32_sinhala_ci", false);
    CHARSETS[180] = new MySQLCharset(180, "utf32", "utf32_german2_ci", false);
    CHARSETS[181] = new MySQLCharset(181, "utf32", "utf32_croatian_ci", false);
    CHARSETS[182] = new MySQLCharset(182, "utf32", "utf32_unicode_520_ci", false);
    CHARSETS[183] = new MySQLCharset(183, "utf32", "utf32_vietnamese_ci", false);
    CHARSETS[192] = new MySQLCharset(192, "utf8", "utf8_unicode_ci", false);
    CHARSETS[193] = new MySQLCharset(193, "utf8", "utf8_icelandic_ci", false);
    CHARSETS[194] = new MySQLCharset(194, "utf8", "utf8_latvian_ci", false);
    CHARSETS[195] = new MySQLCharset(195, "utf8", "utf8_romanian_ci", false);
    CHARSETS[196] = new MySQLCharset(196, "utf8", "utf8_slovenian_ci", false);
    CHARSETS[197] = new MySQLCharset(197, "utf8", "utf8_polish_ci", false);
    CHARSETS[198] = new MySQLCharset(198, "utf8", "utf8_estonian_ci", false);
    CHARSETS[199] = new MySQLCharset(199, "utf8", "utf8_spanish_ci", false);
    CHARSETS[200] = new MySQLCharset(200, "utf8", "utf8_swedish_ci", false);
    CHARSETS[201] = new MySQLCharset(201, "utf8", "utf8_turkish_ci", false);
    CHARSETS[202] = new MySQLCharset(202, "utf8", "utf8_czech_ci", false);
    CHARSETS[203] = new MySQLCharset(203, "utf8", "utf8_danish_ci", false);
    CHARSETS[204] = new MySQLCharset(204, "utf8", "utf8_lithuanian_ci", false);
    CHARSETS[205] = new MySQLCharset(205, "utf8", "utf8_slovak_ci", false);
    CHARSETS[206] = new MySQLCharset(206, "utf8", "utf8_spanish2_ci", false);
    CHARSETS[207] = new MySQLCharset(207, "utf8", "utf8_roman_ci", false);
    CHARSETS[208] = new MySQLCharset(208, "utf8", "utf8_persian_ci", false);
    CHARSETS[209] = new MySQLCharset(209, "utf8", "utf8_esperanto_ci", false);
    CHARSETS[210] = new MySQLCharset(210, "utf8", "utf8_hungarian_ci", false);
    CHARSETS[211] = new MySQLCharset(211, "utf8", "utf8_sinhala_ci", false);
    CHARSETS[212] = new MySQLCharset(212, "utf8", "utf8_german2_ci", false);
    CHARSETS[213] = new MySQLCharset(213, "utf8", "utf8_croatian_ci", false);
    CHARSETS[214] = new MySQLCharset(214, "utf8", "utf8_unicode_520_ci", false);
    CHARSETS[215] = new MySQLCharset(215, "utf8", "utf8_vietnamese_ci", false);
    CHARSETS[223] = new MySQLCharset(223, "utf8", "utf8_general_mysql500_ci", false);
    CHARSETS[224] = new MySQLCharset(224, "utf8mb4", "utf8mb4_unicode_ci", false);
    CHARSETS[225] = new MySQLCharset(225, "utf8mb4", "utf8mb4_icelandic_ci", false);
    CHARSETS[226] = new MySQLCharset(226, "utf8mb4", "utf8mb4_latvian_ci", false);
    CHARSETS[227] = new MySQLCharset(227, "utf8mb4", "utf8mb4_romanian_ci", false);
    CHARSETS[228] = new MySQLCharset(228, "utf8mb4", "utf8mb4_slovenian_ci", false);
    CHARSETS[229] = new MySQLCharset(229, "utf8mb4", "utf8mb4_polish_ci", false);
    CHARSETS[230] = new MySQLCharset(230, "utf8mb4", "utf8mb4_estonian_ci", false);
    CHARSETS[231] = new MySQLCharset(231, "utf8mb4", "utf8mb4_spanish_ci", false);
    CHARSETS[232] = new MySQLCharset(232, "utf8mb4", "utf8mb4_swedish_ci", false);
    CHARSETS[233] = new MySQLCharset(233, "utf8mb4", "utf8mb4_turkish_ci", false);
    CHARSETS[234] = new MySQLCharset(234, "utf8mb4", "utf8mb4_czech_ci", false);
    CHARSETS[235] = new MySQLCharset(235, "utf8mb4", "utf8mb4_danish_ci", false);
    CHARSETS[236] = new MySQLCharset(236, "utf8mb4", "utf8mb4_lithuanian_ci", false);
    CHARSETS[237] = new MySQLCharset(237, "utf8mb4", "utf8mb4_slovak_ci", false);
    CHARSETS[238] = new MySQLCharset(238, "utf8mb4", "utf8mb4_spanish2_ci", false);
    CHARSETS[239] = new MySQLCharset(239, "utf8mb4", "utf8mb4_roman_ci", false);
    CHARSETS[240] = new MySQLCharset(240, "utf8mb4", "utf8mb4_persian_ci", false);
    CHARSETS[241] = new MySQLCharset(241, "utf8mb4", "utf8mb4_esperanto_ci", false);
    CHARSETS[242] = new MySQLCharset(242, "utf8mb4", "utf8mb4_hungarian_ci", false);
    CHARSETS[243] = new MySQLCharset(243, "utf8mb4", "utf8mb4_sinhala_ci", false);
    CHARSETS[244] = new MySQLCharset(244, "utf8mb4", "utf8mb4_german2_ci", false);
    CHARSETS[245] = new MySQLCharset(245, "utf8mb4", "utf8mb4_croatian_ci", false);
    CHARSETS[246] = new MySQLCharset(246, "utf8mb4", "utf8mb4_unicode_520_ci", false);
    CHARSETS[247] = new MySQLCharset(247, "utf8mb4", "utf8mb4_vietnamese_ci", false);
    CHARSETS[248] = new MySQLCharset(248, "gb18030", "gb18030_chinese_ci", true);
    CHARSETS[249] = new MySQLCharset(249, "gb18030", "gb18030_bin", false);
    CHARSETS[250] = new MySQLCharset(250, "gb18030", "gb18030_unicode_520_ci", false);
    CHARSETS[255] = new MySQLCharset(255, "utf8mb4", "utf8mb4_0900_ai_ci", true);
    CHARSETS[256] = new MySQLCharset(256, "utf8mb4", "utf8mb4_de_pb_0900_ai_ci", false);
    CHARSETS[257] = new MySQLCharset(257, "utf8mb4", "utf8mb4_is_0900_ai_ci", false);
    CHARSETS[258] = new MySQLCharset(258, "utf8mb4", "utf8mb4_lv_0900_ai_ci", false);
    CHARSETS[259] = new MySQLCharset(259, "utf8mb4", "utf8mb4_ro_0900_ai_ci", false);
    CHARSETS[260] = new MySQLCharset(260, "utf8mb4", "utf8mb4_sl_0900_ai_ci", false);
    CHARSETS[261] = new MySQLCharset(261, "utf8mb4", "utf8mb4_pl_0900_ai_ci", false);
    CHARSETS[262] = new MySQLCharset(262, "utf8mb4", "utf8mb4_et_0900_ai_ci", false);
    CHARSETS[263] = new MySQLCharset(263, "utf8mb4", "utf8mb4_es_0900_ai_ci", false);
    CHARSETS[264] = new MySQLCharset(264, "utf8mb4", "utf8mb4_sv_0900_ai_ci", false);
    CHARSETS[265] = new MySQLCharset(265, "utf8mb4", "utf8mb4_tr_0900_ai_ci", false);
    CHARSETS[266] = new MySQLCharset(266, "utf8mb4", "utf8mb4_cs_0900_ai_ci", false);
    CHARSETS[267] = new MySQLCharset(267, "utf8mb4", "utf8mb4_da_0900_ai_ci", false);
    CHARSETS[268] = new MySQLCharset(268, "utf8mb4", "utf8mb4_lt_0900_ai_ci", false);
    CHARSETS[269] = new MySQLCharset(269, "utf8mb4", "utf8mb4_sk_0900_ai_ci", false);
    CHARSETS[270] = new MySQLCharset(270, "utf8mb4", "utf8mb4_es_trad_0900_ai_ci", false);
    CHARSETS[271] = new MySQLCharset(271, "utf8mb4", "utf8mb4_la_0900_ai_ci", false);
    CHARSETS[273] = new MySQLCharset(273, "utf8mb4", "utf8mb4_eo_0900_ai_ci", false);
    CHARSETS[274] = new MySQLCharset(274, "utf8mb4", "utf8mb4_hu_0900_ai_ci", false);
    CHARSETS[275] = new MySQLCharset(275, "utf8mb4", "utf8mb4_hr_0900_ai_ci", false);
    CHARSETS[277] = new MySQLCharset(277, "utf8mb4", "utf8mb4_vi_0900_ai_ci", false);
    CHARSETS[278] = new MySQLCharset(278, "utf8mb4", "utf8mb4_0900_as_cs", false);
    CHARSETS[279] = new MySQLCharset(279, "utf8mb4", "utf8mb4_de_pb_0900_as_cs", false);
    CHARSETS[280] = new MySQLCharset(280, "utf8mb4", "utf8mb4_is_0900_as_cs", false);
    CHARSETS[281] = new MySQLCharset(281, "utf8mb4", "utf8mb4_lv_0900_as_cs", false);
    CHARSETS[282] = new MySQLCharset(282, "utf8mb4", "utf8mb4_ro_0900_as_cs", false);
    CHARSETS[283] = new MySQLCharset(283, "utf8mb4", "utf8mb4_sl_0900_as_cs", false);
    CHARSETS[284] = new MySQLCharset(284, "utf8mb4", "utf8mb4_pl_0900_as_cs", false);
    CHARSETS[285] = new MySQLCharset(285, "utf8mb4", "utf8mb4_et_0900_as_cs", false);
    CHARSETS[286] = new MySQLCharset(286, "utf8mb4", "utf8mb4_es_0900_as_cs", false);
    CHARSETS[287] = new MySQLCharset(287, "utf8mb4", "utf8mb4_sv_0900_as_cs", false);
    CHARSETS[288] = new MySQLCharset(288, "utf8mb4", "utf8mb4_tr_0900_as_cs", false);
    CHARSETS[289] = new MySQLCharset(289, "utf8mb4", "utf8mb4_cs_0900_as_cs", false);
    CHARSETS[290] = new MySQLCharset(290, "utf8mb4", "utf8mb4_da_0900_as_cs", false);
    CHARSETS[291] = new MySQLCharset(291, "utf8mb4", "utf8mb4_lt_0900_as_cs", false);
    CHARSETS[292] = new MySQLCharset(292, "utf8mb4", "utf8mb4_sk_0900_as_cs", false);
    CHARSETS[293] = new MySQLCharset(293, "utf8mb4", "utf8mb4_es_trad_0900_as_cs", false);
    CHARSETS[294] = new MySQLCharset(294, "utf8mb4", "utf8mb4_la_0900_as_cs", false);
    CHARSETS[296] = new MySQLCharset(296, "utf8mb4", "utf8mb4_eo_0900_as_cs", false);
    CHARSETS[297] = new MySQLCharset(297, "utf8mb4", "utf8mb4_hu_0900_as_cs", false);
    CHARSETS[298] = new MySQLCharset(298, "utf8mb4", "utf8mb4_hr_0900_as_cs", false);
    CHARSETS[300] = new MySQLCharset(300, "utf8mb4", "utf8mb4_vi_0900_as_cs", false);
    CHARSETS[303] = new MySQLCharset(303, "utf8mb4", "utf8mb4_ja_0900_as_cs", false);
    CHARSETS[304] = new MySQLCharset(304, "utf8mb4", "utf8mb4_ja_0900_as_cs_ks", false);
    CHARSETS[305] = new MySQLCharset(305, "utf8mb4", "utf8mb4_0900_as_ci", false);
    CHARSETS[306] = new MySQLCharset(306, "utf8mb4", "utf8mb4_ru_0900_ai_ci", false);
    CHARSETS[307] = new MySQLCharset(307, "utf8mb4", "utf8mb4_ru_0900_as_cs", false);
    CHARSETS[308] = new MySQLCharset(308, "utf8mb4", "utf8mb4_zh_0900_as_cs", false);
    CHARSETS[309] = new MySQLCharset(309, "utf8mb4", "utf8mb4_0900_bin", false);
  }

  public static MySQLCharset getCharsetById(final int id) {
    var charset = CHARSETS[id];
    if (charset == null) {
      throw new IllegalArgumentException(String.format("Invalid charset id, [%s]", id));
    }
    return charset;
  }

  public static MySQLCharset getCharsetByName(final String name) {
    MySQLCharset nonDefault = null;
    var lowerName = name.toLowerCase();
    for (var charset: CHARSETS) {
      if (charset.name.equals(lowerName)) {
        if (charset.isDefault) {
          return charset;
        }
        if (nonDefault == null) {
          nonDefault = charset;
        }
      }
    }
    if (nonDefault == null) {
      throw new IllegalArgumentException(String.format("Invalid charset name, [%s]", lowerName));
    }

    return nonDefault;
  }

  @Getter
  public static final class MySQLCharset {
    private final int id;
    private final String name;
    private final String encoding;
    private final String collation;
    private final boolean isDefault;

    private MySQLCharset(
        final int id, final String name, final String collation, final boolean isDefault) {
      this.id = id;
      this.name = name;
      this.collation = collation;
      this.isDefault = isDefault;
      if (UTF8MB4.equals(name)) {
        this.encoding = UTF8;
      } else {
        this.encoding = name;
      }
    }

    public Charset charset() {
      return Charset.forName(encoding);
    }
  }
}
