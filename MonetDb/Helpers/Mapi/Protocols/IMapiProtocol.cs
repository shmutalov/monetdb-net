using System.Runtime.InteropServices;

namespace System.Data.MonetDb.Helpers.Mapi.Protocols
{
    /// <summary>
    /// Interface for defining Mapi protocols
    /// </summary>
    internal interface IMapiProtocol
    {
        /// <summary>
        /// Build challenge response string from input parameters
        /// </summary>
        /// <param name="userName">Username, may changed if server identifies self as Merovingian</param>
        /// <param name="password">Password, hashed password will be returned as out</param>
        /// <param name="language">Mapi quering language name</param>
        /// <param name="challengeTokens"></param>
        /// <param name="database">Database, can be <c>null</c></param>
        /// <param name="hash">Default for hashing algorithm will used if set</param>
        /// <returns></returns>
        string BuildChallengeResponse(
            [In, Out] string userName, 
            [In, Out] string password,
            string language,
            string[] challengeTokens,
            string database = null,
            string hash = null);
    }
}
