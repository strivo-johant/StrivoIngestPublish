using StrivoIngestPublish;
using Xunit;

namespace StrivoIngestPublish.Tests;

public class CsvMessageBuilderTests
{
    // --- ParseCsvLine tests ---

    [Fact]
    public void ParseCsvLine_SimpleFields_ReturnsValues()
    {
        var result = CsvMessageBuilder.ParseCsvLine("1,Hello,Extra");
        Assert.Equal(["1", "Hello", "Extra"], result);
    }

    [Fact]
    public void ParseCsvLine_FieldWithLeadingSpace_PreservesWhitespace()
    {
        // RFC 4180: whitespace is part of the field value and must not be trimmed
        var result = CsvMessageBuilder.ParseCsvLine("1, Hello ,Extra");
        Assert.Equal(["1", " Hello ", "Extra"], result);
    }

    [Fact]
    public void ParseCsvLine_QuotedFieldWithComma_TreatedAsSingleField()
    {
        var result = CsvMessageBuilder.ParseCsvLine("1,\"last, first\",Extra");
        Assert.Equal(["1", "last, first", "Extra"], result);
    }

    [Fact]
    public void ParseCsvLine_EscapedDoubleQuoteInField_PreservesQuote()
    {
        var result = CsvMessageBuilder.ParseCsvLine("1,\"say \"\"hello\"\"\",Extra");
        Assert.Equal(["1", "say \"hello\"", "Extra"], result);
    }

    [Fact]
    public void ParseCsvLine_EmptyField_ReturnsEmptyString()
    {
        var result = CsvMessageBuilder.ParseCsvLine("1,,Extra");
        Assert.Equal(["1", "", "Extra"], result);
    }

    [Fact]
    public void ParseCsvLine_SingleField_ReturnsSingleElement()
    {
        var result = CsvMessageBuilder.ParseCsvLine("only");
        Assert.Equal(["only"], result);
    }

    // --- GenerateDefaultHeaders tests ---

    [Fact]
    public void GenerateDefaultHeaders_ZeroColumns_ReturnsEmptyArray()
    {
        Assert.Empty(CsvMessageBuilder.GenerateDefaultHeaders(0));
    }

    [Fact]
    public void GenerateDefaultHeaders_OneColumn_ReturnsId()
    {
        Assert.Equal(["Id"], CsvMessageBuilder.GenerateDefaultHeaders(1));
    }

    [Fact]
    public void GenerateDefaultHeaders_TwoColumns_ReturnsIdAndMessage()
    {
        Assert.Equal(["Id", "Message"], CsvMessageBuilder.GenerateDefaultHeaders(2));
    }

    [Fact]
    public void GenerateDefaultHeaders_FourColumns_ReturnsIdMessageMessage2Message3()
    {
        Assert.Equal(["Id", "Message", "Message2", "Message3"], CsvMessageBuilder.GenerateDefaultHeaders(4));
    }

    // --- EscapeJson tests ---

    [Fact]
    public void EscapeJson_NoSpecialChars_ReturnsSameValue()
    {
        Assert.Equal("hello", CsvMessageBuilder.EscapeJson("hello"));
    }

    [Fact]
    public void EscapeJson_DoubleQuote_EscapesQuote()
    {
        Assert.Equal("say \\\"hi\\\"", CsvMessageBuilder.EscapeJson("say \"hi\""));
    }

    [Fact]
    public void EscapeJson_Backslash_EscapesBackslash()
    {
        Assert.Equal("a\\\\b", CsvMessageBuilder.EscapeJson("a\\b"));
    }

    // --- BuildMessage tests ---

    [Fact]
    public void BuildMessage_TwoColumnRow_UsesIdAndMessageKeys()
    {
        var message = CsvMessageBuilder.BuildMessage("42,Hello world", "data.csv");

        Assert.StartsWith("{", message);
        Assert.EndsWith("}", message);
        Assert.Contains("\"source\":\"data.csv\"", message);
        Assert.Contains("\"Id\":\"42\"", message);
        Assert.Contains("\"Message\":\"Hello world\"", message);
    }

    [Fact]
    public void BuildMessage_ThreeColumnRow_UsesIdMessageMessage2()
    {
        var message = CsvMessageBuilder.BuildMessage("1,Hello,Extra", "data.csv");

        Assert.Contains("\"Id\":\"1\"", message);
        Assert.Contains("\"Message\":\"Hello\"", message);
        Assert.Contains("\"Message2\":\"Extra\"", message);
    }

    [Fact]
    public void BuildMessage_FourColumnRow_UsesIdMessageMessage2Message3()
    {
        var message = CsvMessageBuilder.BuildMessage("1,Hello,Extra,More", "data.csv");

        Assert.Contains("\"Id\":\"1\"", message);
        Assert.Contains("\"Message\":\"Hello\"", message);
        Assert.Contains("\"Message2\":\"Extra\"", message);
        Assert.Contains("\"Message3\":\"More\"", message);
    }

    [Fact]
    public void BuildMessage_SourceBlobName_IsEscapedInJson()
    {
        var message = CsvMessageBuilder.BuildMessage("1,Hello", "path/to/my \"file\".csv");

        Assert.Contains("\"source\":\"path/to/my \\\"file\\\".csv\"", message);
    }

    [Fact]
    public void BuildMessage_QuotedCsvField_IsUnquotedInJson()
    {
        var message = CsvMessageBuilder.BuildMessage("1,\"New York\"", "data.csv");

        Assert.Contains("\"Message\":\"New York\"", message);
    }
}
