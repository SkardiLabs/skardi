{ pkgs, config, ... }:
{
  dotenv.enable = false;

  languages.rust = {
    enable = true;
  };

  packages = with pkgs; [
    bash
    pkg-config
    openssl
    sqlite
    protobuf
    cmake
    zlib
  ] ++ pkgs.lib.optionals pkgs.stdenv.isLinux [ pkgs.onnxruntime ];

  enterShell = pkgs.lib.optionalString pkgs.stdenv.isLinux ''
    export ORT_LIB_LOCATION="${pkgs.onnxruntime}/lib"
    export ORT_PREFER_DYNAMIC_LINK=1
    export LD_LIBRARY_PATH="${pkgs.onnxruntime}/lib:$LD_LIBRARY_PATH"
  '';
}
