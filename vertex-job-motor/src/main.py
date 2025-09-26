# /vertex-job-motor/src/main.py

import sys
import argparse
# Adiciona o diretório atual ao path para encontrar seus módulos
sys.path.append(".") 

# Importa as funções principais
from modelo_nivel_par import execucao_motor
from aws_functions import insert_gcs_parquet # Mantenha se precisar salvar no GCS

def run_job(output_bucket: str):
    """Executa o modelo e salva o resultado."""
    print("--- 1. Iniciando execução do motor de previsão ---")
    saida_modelo = execucao_motor()
    
    print("--- 2. Salvando resultado no GCS ---")
    path = 'Saida_motor/'
    insert_job = insert_gcs_parquet(
        input_dataframe=saida_modelo, 
        bucket=output_bucket, 
        path=path
    )
    print(f"Resultado salvo no GCS: gs://{output_bucket}/{insert_job}")
    
    # Você pode retornar o caminho completo do arquivo se for usar um Pipeline
    return f"gs://{output_bucket}/{insert_job}"

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    # Adicionamos um argumento para passar o bucket de destino (melhor prática)
    parser.add_argument(
        '--output_bucket', 
        type=str, 
        default='staging-motor-nivel-par', 
        help='Bucket GCS para salvar o resultado do modelo.'
    )
    args = parser.parse_args()
    run_job(args.output_bucket)