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
    onnxruntime
  ];

  enterShell = ''
    export ORT_LIB_LOCATION="${pkgs.onnxruntime}/lib"
    export ORT_PREFER_DYNAMIC_LINK=1
    export LD_LIBRARY_PATH="${pkgs.onnxruntime}/lib:$LD_LIBRARY_PATH"
  '';
}
