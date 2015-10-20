namespace System.Data.MonetDb.Extensions
{
    internal static class FormatExt
    {
        public static string ToHex(this byte[] bytes)
        {
            var retval = new string[bytes.Length];

            for (var i = 0; i < bytes.Length; i++)
                retval[i] = bytes[i].ToString("X2");

            return string.Join("", retval);
        }

        public static string ToHexDigest(this byte[] bytes)
        {
            return BitConverter.ToString(bytes).Replace("-", "").ToLower();
        }
    }
}
