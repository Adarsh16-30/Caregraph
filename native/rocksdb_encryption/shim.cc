// Bridges CareGraph's Rust storage layer to RocksDB's *real* C++
// encryption-at-rest API (rocksdb::EncryptionProvider / NewEncryptedEnv),
// which the `rocksdb` Rust crate does not expose at all — its C API
// (librocksdb-sys) never wraps env_encryption.h, even though the C++ core it
// links is compiled with it (env/env_encryption.cc is in rocksdb_lib_sources).
//
// RocksDB's own built-in cipher (ROT13BlockCipher) is explicitly documented
// as "only suitable for test purposes and should not be used in production"
// (env_encryption.h) — there is no built-in AES support in this build (no
// OpenSSL plugin compiled in), so this shim supplies a BlockCipher backed by
// CareGraph's own AES-256 implementation (aes256.h, verified against
// FIPS-197 test vectors) and wires it into RocksDB's stock CTREncryptionProvider.
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

#include "rocksdb/env.h"
#include "rocksdb/env_encryption.h"
#include "rocksdb/status.h"

#include "aes256.h"

namespace {

// Mirrors db/c.cc's private struct exactly (see file header comment) so a
// pointer this shim returns is destroyable via the stock C API function
// rocksdb_env_destroy, which the safe Rust `Env`'s Drop impl calls.
struct rocksdb_env_t {
  ROCKSDB_NAMESPACE::Env* rep;
  bool is_default;
};

// A RocksDB BlockCipher backed by CareGraph's own AES-256 (aes256.h).
// CTREncryptionProvider calls Encrypt() once per 16-byte counter block to
// build its keystream — see rocksdb/env_encryption.h's BlockCipher doc.
class CareGraphAes256Cipher : public ROCKSDB_NAMESPACE::BlockCipher {
 public:
  explicit CareGraphAes256Cipher(const uint8_t key[caregraph_crypto::Aes256::kKeyBytes])
      : aes_(key) {}

  const char* Name() const override { return "CareGraphAES256"; }

  size_t BlockSize() override { return caregraph_crypto::Aes256::kBlockBytes; }

  ROCKSDB_NAMESPACE::Status Encrypt(char* data) override {
    aes_.EncryptBlock(reinterpret_cast<uint8_t*>(data));
    return ROCKSDB_NAMESPACE::Status::OK();
  }

  ROCKSDB_NAMESPACE::Status Decrypt(char* data) override {
    aes_.DecryptBlock(reinterpret_cast<uint8_t*>(data));
    return ROCKSDB_NAMESPACE::Status::OK();
  }

 private:
  caregraph_crypto::Aes256 aes_;
};

}  // namespace

extern "C" {

// Builds a real, RocksDB-native encrypted Env (CTR mode over AES-256) and
// returns it as a rocksdb_env_t* compatible with the `rocksdb` crate's
// `Env::from_raw` / `Options::set_env`. `key` must point to exactly 32 bytes
// (AES-256); the caller (src/storage/encryption.rs) validates this before
// calling — a wrong-length key here is a caller bug, not a runtime option,
// so this asserts rather than returning null.
//
// Returns null only if RocksDB itself refuses to construct the provider or
// the encrypted env (should not happen with a valid key; checked anyway so
// a future RocksDB upgrade that changes this contract fails loudly instead
// of dereferencing null downstream).
rocksdb_env_t* caregraph_create_aes256_ctr_encrypted_env(const uint8_t* key,
                                                           size_t key_len) {
  if (key == nullptr || key_len != caregraph_crypto::Aes256::kKeyBytes) {
    return nullptr;
  }

  auto cipher = std::make_shared<CareGraphAes256Cipher>(key);
  std::shared_ptr<ROCKSDB_NAMESPACE::EncryptionProvider> provider =
      ROCKSDB_NAMESPACE::EncryptionProvider::NewCTRProvider(cipher);
  if (!provider) {
    return nullptr;
  }

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
