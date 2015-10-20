using System.Data.MonetDb.Extensions;
using System.Security.Cryptography;
using System.Text;

namespace System.Data.MonetDb.Helpers.Mapi.Protocols
{
    /// <summary>
    /// Mapi protocol version 9
    /// </summary>
    internal class MapiProtocolVersion9 : IMapiProtocol
    {
        public string BuildChallengeResponse(string userName, string password, 
            string language, string[] challengeTokens,
            string database = null, string hash = null)
        {
            if (challengeTokens.Length < 6)
                throw new MonetDbException("Not enough parameters for building challenge response");

            var salt = challengeTokens[0];
            var serverType = challengeTokens[1];
            var hashes = (hash ?? challengeTokens[3]).Split(new [] {','}, StringSplitOptions.RemoveEmptyEntries);

            // Protocol version 9 sends to us first step password hashing algorithm
            var algorithm = challengeTokens[5].ToUpper();

            var hasher = HashAlgorithm.Create(algorithm);

            if (hasher == null)
                throw new MonetDbException("Hashing algorithm {0} is not supported by this platform", 
                    algorithm);

            hasher.Initialize();
            password = hasher.ComputeHash(Encoding.UTF8.GetBytes(password)).ToHexDigest();

            // if we deal with merovingian, mask our credentials
            if (serverType.Equals("merovingian") && !language.Equals("control"))
            {
                userName = "merovingian";
                password = "merovingian";
            }

            string foundAlgo;

            if (!hashes.In(out foundAlgo, 
                "SHA512",
                "SHA384",
                "SHA256",
                "SHA1",
                "MD5"))
            {
                throw new MonetDbException("No supported hashes in {0}", hashes);
            }
            
            hasher = HashAlgorithm.Create(foundAlgo);

            if (hasher == null)
            {
                throw new MonetDbException("Hashing algorithm {0} is not supported by this platform",
                    foundAlgo);
            }

            hasher.Initialize();

            var pwdBytes = Encoding.UTF8.GetBytes(password);
            var saltBytes = Encoding.UTF8.GetBytes(salt);

            hasher.TransformBlock(pwdBytes, 0, pwdBytes.Length, pwdBytes, 0);
            hasher.TransformFinalBlock(saltBytes, 0, saltBytes.Length);

            var pwhash = string.Format("{{{0}}}{1}",
                foundAlgo,
                hasher.Hash.ToHexDigest());

            return string.Format("{0}:{1}:{2}:{3}:{4}:",
                BitConverter.IsLittleEndian ? "LIT" : "BIG",
                userName, pwhash, language, database);
        }
    }
}
