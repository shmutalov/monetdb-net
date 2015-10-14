using System;
using System.Collections.Generic;
using System.Data.MonetDb.Extensions;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;

namespace System.Data.MonetDb.Helpers.Mapi.Protocols
{
    /// <summary>
    /// Mapi protocol version 8
    /// </summary>
    internal class MapiProtocolVersion8 : IMapiProtocol
    {
        public string BuildChallengeResponse(string userName, string password, 
            string language, string[] challengeTokens,
            string database = null, string hash = null)
        {
            if (challengeTokens.Length < 5)
                throw new MonetDbException("Not enough parameters for building challenge response");

            var salt = challengeTokens[0];
            var serverType = challengeTokens[1];
            var hashes = (hash ?? challengeTokens[3]).Split(new[] { ',' }, StringSplitOptions.RemoveEmptyEntries);

            // if we deal with merovingian, mask our credentials
            if (serverType.Equals("merovingian") && !language.Equals("control"))
            {
                userName = "merovingian";
                password = "merovingian";
            }

            string pwhash;

            string foundAlgo;

            if (!hashes.In(out foundAlgo, "SHA1", "MD5", "plain"))
            {
                throw new MonetDbException("No supported hashes in {0}", hashes);
            }

            if (foundAlgo.Equals("plain"))
            {
                pwhash = string.Format("{0}{1}{2}", "{plain}", password, salt);
            }
            else
            {
                var hasher = HashAlgorithm.Create(foundAlgo);

                if (hasher == null)
                {
                    throw new MonetDbException("Hashing algorithm {0} is not supported by this platform",
                        foundAlgo);
                }

                hasher.Initialize();

                pwhash = string.Format("{{{0}}}{1}",
                    foundAlgo, 
                    hasher.ComputeHash(Encoding.UTF8.GetBytes(password + salt)).ToHex());
            }

            // In proto 8 byte-order of the blocks is always little endian
            return string.Format("{0}:{1}:{2}:{3}:{4}:",
                "LIT",  
                userName, pwhash, language, database);
        }
    }
}
