// Bridges CareGraph's Rust storage layer to RocksDB's *real* C++
// encryption-at-rest API (rocksdb::EncryptionProvider / NewEncryptedEnv),
// which the `rocksdb` Rust crate does not expose at all — its C API
// (librocksdb-sys) never wraps env_encryption.h, even though the C++ core it
// links is compiled with it (env/env_encryption.cc is in rocksdb_lib_sources).
//
// RocksDB's own built-in cipher (ROT13BlockCipher) is explicitly documented
// as "only suitable for test purposes and should not be used in production"
// (env_encryption.h), and this build has no OpenSSL plugin. So the block cipher
// is supplied by the caller: the Rust side hands in a table of function
// pointers over the RustCrypto `aes` crate (hardware AES-NI where available,
// constant-time software otherwise), and this shim wraps them as a
// BlockCipher feeding RocksDB's stock CTREncryptionProvider. No cipher code
// lives in C++.
//
// Returned rocksdb_env_t* objects match the layout RocksDB's own C API
// (db/c.cc) uses internally — `struct rocksdb_env_t { Env* rep; bool
// is_default; };` — so the safe `rocksdb` crate's `Env::from_raw` /
// `rocksdb_env_destroy` (called from EnvWrapper::drop) work on them exactly
// as they would on an Env the C API created itself. This struct has been
// part of RocksDB's stable C ABI for years; see db/c.cc's own definition.
#include <cstdint>
#include <cstring>
#include <memory>
#include <string>
#include <utility>

#include "rocksdb/env.h"
#include "rocksdb/env_encryption.h"
#include "rocksdb/status.h"

namespace {

// Mirrors db/c.cc's private struct exactly (see file header comment) so a
// pointer this shim returns is destroyable via the stock C API function
// rocksdb_env_destroy, which the safe Rust `Env`'s Drop impl calls.
struct rocksdb_env_t {
  ROCKSDB_NAMESPACE::Env* rep;
  bool is_default;
};

// The cipher the Rust side supplies: an opaque context plus encrypt/decrypt of
// one 16-byte block in place, and a destructor for the context. Layout must
// match `CipherOps` in src/storage/encryption.rs.
struct caregraph_cipher_ops {
  void* ctx;
  void (*encrypt)(void* ctx, uint8_t* block);
  void (*decrypt)(void* ctx, uint8_t* block);
  void (*destroy)(void* ctx);
  // Fills `buf[0..len)` from the operating system's cryptographic random number
  // generator. Returns 0 on success, non-zero on failure. Takes no context: it
  // reads no key material. Used only to make each new file's prefix (below).
  int (*fill_random)(uint8_t* buf, size_t len);
};

// The Rust side declares the same table (`CipherOps` in src/storage/encryption.rs); a
// mismatch would be silent memory corruption, so both sides assert the layout.
static_assert(sizeof(caregraph_cipher_ops) == 5 * sizeof(void*),
              "caregraph_cipher_ops must match CipherOps in src/storage/encryption.rs");

constexpr size_t kBlockBytes = 16;

// A RocksDB BlockCipher that forwards to the Rust-supplied function table.
// CTREncryptionProvider calls Encrypt() once per 16-byte counter block to
// build its keystream — see rocksdb/env_encryption.h's BlockCipher doc. Owns
// the context: the destructor hands it back to Rust to be freed (and its key
// material zeroized).
class RustBlockCipher : public ROCKSDB_NAMESPACE::BlockCipher {
 public:
  explicit RustBlockCipher(const caregraph_cipher_ops& ops) : ops_(ops) {}
  ~RustBlockCipher() override { ops_.destroy(ops_.ctx); }
  RustBlockCipher(const RustBlockCipher&) = delete;
  RustBlockCipher& operator=(const RustBlockCipher&) = delete;

  const char* Name() const override { return "CareGraphAES256"; }

  size_t BlockSize() override { return kBlockBytes; }

  ROCKSDB_NAMESPACE::Status Encrypt(char* data) override {
    ops_.encrypt(ops_.ctx, reinterpret_cast<uint8_t*>(data));
    return ROCKSDB_NAMESPACE::Status::OK();
  }

  ROCKSDB_NAMESPACE::Status Decrypt(char* data) override {
    ops_.decrypt(ops_.ctx, reinterpret_cast<uint8_t*>(data));
    return ROCKSDB_NAMESPACE::Status::OK();
  }

 private:
  caregraph_cipher_ops ops_;
};

// RocksDB's stock CTR provider is used for everything except how a new file's
// prefix is made.
//
// The stock CreateNewPrefix fills a file's initial counter and IV from RocksDB's
// `Random`, seeded with `(uint32_t)NowMicros()` (env/env_encryption.cc). That generator
// keeps 31 bits of state, so at most 2^31 different prefixes exist however many files
// are written, and two files created at the same microsecond modulo 2^31 get the same
// counter and IV, hence the same keystream. XOR of their ciphertexts is then the XOR
// of their plaintexts, with no key needed. By the birthday bound that is about a 2%
// chance at 10,000 files over a database's life and expected at 100,000, which flush and
// compaction churn reaches.
//
// Here the prefix comes from the OS CSPRNG (through `fill_random`, implemented in Rust),
// and file creation fails if it cannot be read, rather than falling back to weak
// randomness. A file's prefix is stored in the file, so files written by the stock
// provider still decrypt: only new files change. The rest of the prefix (past the two
// blocks that hold the counter and IV) is random filler the stock provider also fills
// with random bytes (its `PopulateSecretPrefixPart` is a no-op), so filling all of it
// is equivalent.
class SecurePrefixProvider : public ROCKSDB_NAMESPACE::EncryptionProvider {
 public:
  SecurePrefixProvider(std::shared_ptr<ROCKSDB_NAMESPACE::EncryptionProvider> stock,
                       int (*fill_random)(uint8_t*, size_t))
      : stock_(std::move(stock)), fill_random_(fill_random) {}

  const char* Name() const override { return stock_->Name(); }
  size_t GetPrefixLength() const override { return stock_->GetPrefixLength(); }
  std::string GetMarker() const override { return stock_->GetMarker(); }

  ROCKSDB_NAMESPACE::Status CreateNewPrefix(const std::string& /*fname*/, char* prefix,
                                            size_t prefix_length) const override {
    if (prefix_length < 2 * kBlockBytes) {
      return ROCKSDB_NAMESPACE::Status::InvalidArgument(
          "encryption prefix is shorter than the counter and IV blocks");
    }
    if (fill_random_(reinterpret_cast<uint8_t*>(prefix), prefix_length) != 0) {
      return ROCKSDB_NAMESPACE::Status::IOError(
          "the operating system's random number generator failed; refusing to create "
          "an encrypted file with a weak prefix");
    }
    return ROCKSDB_NAMESPACE::Status::OK();
  }

  ROCKSDB_NAMESPACE::Status AddCipher(const std::string& descriptor, const char* cipher,
                                      size_t len, bool for_write) override {
    return stock_->AddCipher(descriptor, cipher, len, for_write);
  }

  ROCKSDB_NAMESPACE::Status CreateCipherStream(
      const std::string& fname, const ROCKSDB_NAMESPACE::EnvOptions& options,
      ROCKSDB_NAMESPACE::Slice& prefix,
      std::unique_ptr<ROCKSDB_NAMESPACE::BlockAccessCipherStream>* result) override {
    return stock_->CreateCipherStream(fname, options, prefix, result);
  }

 private:
  std::shared_ptr<ROCKSDB_NAMESPACE::EncryptionProvider> stock_;
  int (*fill_random_)(uint8_t*, size_t);
};

}  // namespace

extern "C" {

// Builds a real, RocksDB-native encrypted Env (CTR mode over the supplied
// block cipher) and returns it as a rocksdb_env_t* compatible with the
// `rocksdb` crate's `Env::from_raw` / `Options::set_env`. Takes ownership of
// `ops->ctx` in every case, including failure: `ops->destroy` is called exactly
// once, either when the Env is destroyed or before this returns null.
//
// Returns null only if RocksDB itself refuses to construct the provider or
// the encrypted env (should not happen; checked anyway so a future RocksDB
// upgrade that changes this contract fails loudly instead of dereferencing
// null downstream).
rocksdb_env_t* caregraph_create_encrypted_env(const caregraph_cipher_ops* ops) {
  if (ops == nullptr || ops->encrypt == nullptr || ops->decrypt == nullptr ||
      ops->destroy == nullptr) {
    return nullptr;
  }
  if (ops->fill_random == nullptr) {
    // Refuse to build an env that would fall back to the stock, weakly seeded prefix.
    // The contract above says `destroy` is called exactly once on every path.
    ops->destroy(ops->ctx);
    return nullptr;
  }

  auto cipher = std::make_shared<RustBlockCipher>(*ops);
  std::shared_ptr<ROCKSDB_NAMESPACE::EncryptionProvider> stock =
      ROCKSDB_NAMESPACE::EncryptionProvider::NewCTRProvider(cipher);
  if (!stock) {
    return nullptr;
  }
  std::shared_ptr<ROCKSDB_NAMESPACE::EncryptionProvider> provider =
      std::make_shared<SecurePrefixProvider>(std::move(stock), ops->fill_random);

  ROCKSDB_NAMESPACE::Env* base_env = ROCKSDB_NAMESPACE::Env::Default();
  ROCKSDB_NAMESPACE::Env* encrypted_env =
      ROCKSDB_NAMESPACE::NewEncryptedEnv(base_env, provider);
  if (encrypted_env == nullptr) {
    return nullptr;
  }

  rocksdb_env_t* result = new rocksdb_env_t;
  result->rep = encrypted_env;
  // false: rocksdb_env_destroy must `delete result->rep` — this Env owns the
  // provider/cipher (via shared_ptr captured in NewEncryptedEnv's closure
  // internals) and is not the process-wide default Env.
  result->is_default = false;
  return result;
}

}  // extern "C"
