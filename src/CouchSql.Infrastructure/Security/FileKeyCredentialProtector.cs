using System.Security.Cryptography;
using System.Text;
using CouchSql.Core.Interfaces;
using CouchSql.Core.Options;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace CouchSql.Infrastructure.Security;

public sealed class FileKeyCredentialProtector(
    IOptions<SecurityOptions> securityOptions,
    ILogger<FileKeyCredentialProtector> logger) : ICredentialProtector
{
    private readonly string _keyPath = Path.GetFullPath(securityOptions.Value.EncryptionKeyPath, Directory.GetCurrentDirectory());
    private byte[]? _key;

    public string CurrentKeyId => Convert.ToHexString(SHA256.HashData(GetKey())).Substring(0, 16);

    public async Task EnsureKeyExistsAsync(CancellationToken cancellationToken)
    {
        var directory = Path.GetDirectoryName(_keyPath);
        if (!string.IsNullOrWhiteSpace(directory))
        {
            Directory.CreateDirectory(directory);
        }

        if (!File.Exists(_keyPath))
        {
            var keyBytes = RandomNumberGenerator.GetBytes(32);
            await File.WriteAllTextAsync(_keyPath, Convert.ToBase64String(keyBytes), cancellationToken);
            logger.LogInformation("Generated a new credential-encryption key at {KeyPath}", _keyPath);
        }

        _key = Convert.FromBase64String(await File.ReadAllTextAsync(_keyPath, cancellationToken));
    }

    public byte[] Protect(string plaintext)
    {
        var key = GetKey();
        var plaintextBytes = Encoding.UTF8.GetBytes(plaintext);
        var nonce = RandomNumberGenerator.GetBytes(12);
        var ciphertext = new byte[plaintextBytes.Length];
        var tag = new byte[16];

        using var aesGcm = new AesGcm(key, tagSizeInBytes: 16);
        aesGcm.Encrypt(nonce, plaintextBytes, ciphertext, tag);

        var payload = new byte[nonce.Length + ciphertext.Length + tag.Length];
        Buffer.BlockCopy(nonce, 0, payload, 0, nonce.Length);
        Buffer.BlockCopy(ciphertext, 0, payload, nonce.Length, ciphertext.Length);
        Buffer.BlockCopy(tag, 0, payload, nonce.Length + ciphertext.Length, tag.Length);
        return payload;
    }

    public string Unprotect(byte[] ciphertext, string? keyId)
    {
        if (!string.IsNullOrWhiteSpace(keyId) && !string.Equals(keyId, CurrentKeyId, StringComparison.OrdinalIgnoreCase))
        {
            throw new InvalidOperationException("The stored credential key id does not match the active key.");
        }

        var key = GetKey();
        var nonce = ciphertext[..12];
        var tag = ciphertext[^16..];
        var encrypted = ciphertext[12..^16];
        var plaintext = new byte[encrypted.Length];

        using var aesGcm = new AesGcm(key, tagSizeInBytes: 16);
        aesGcm.Decrypt(nonce, encrypted, tag, plaintext);
        return Encoding.UTF8.GetString(plaintext);
    }

    private byte[] GetKey()
    {
        return _key ?? throw new InvalidOperationException("Credential encryption key has not been loaded yet.");
    }
}