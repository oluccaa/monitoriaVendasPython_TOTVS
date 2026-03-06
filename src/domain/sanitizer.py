# src/domain/sanitizer.py

# 1. Imports da Biblioteca Padrão
import hashlib
import re
import unicodedata
from typing import List, Any

# 2. Imports de Terceiros
import pandas as pd

# ==============================================================================
# DOMÍNIO: UTILITÁRIOS DE LIMPEZA DE DADOS
# ==============================================================================

class DataSanitizer:
    """
    Responsável por normalizar e limpar dados brutos vindos de planilhas.
    Blindado contra erros de tipagem comuns em Excel (ex: números em colunas de texto).
    """

    @staticmethod
    def normalize_name(name: Any) -> str:
        """
        Padroniza nomes: Remove acentos, espaços extras e underscores.
        BLINDAGEM: Aceita qualquer tipo de entrada e converte para string segura.
        Ex: 'João_Silva ' -> 'JOAO SILVA'
        Ex: 12345 -> '12345'
        """
        if name is None: return ""
        
        # 1. Blindagem de Tipo (Evita crash se vier int/float do Excel)
        name_str = str(name)
        
        if not name_str.strip(): return ""

        # 2. Remove acentos (Normalização Unicode)
        nksel = unicodedata.normalize('NFKD', name_str)
        name_clean = "".join([c for c in nksel if not unicodedata.combining(c)])
        
        # 3. Transforma em Upper, troca _ por espaço e remove espaços duplicados
        name_clean = name_clean.upper().replace('_', ' ')
        return re.sub(r'\s+', ' ', name_clean).strip()

    @staticmethod
    def clean_numeric(value: Any) -> float:
        """
        Converte valores financeiros "sujos" para float.
        Trata: 'R$ 1.250,50', '1 250.50', vazios, traços e erros de OCR.
        """
        # Verificação rápida de nulidade
        if value is None:
            return 0.0
        if isinstance(value, float) and pd.isna(value):
            return 0.0
            
        # Se já for número (int/float), retorna direto (performance)
        if isinstance(value, (int, float)):
            return float(value)

        # 1. Limpeza básica: Remove R$ e TODOS os espaços (ex: "1 000,00")
        text = str(value).upper().replace('R$', '').replace(' ', '').strip()
        
        # 2. Filtro de "valores vazios/inválidos"
        # Adicionei caracteres invisíveis e zeros comuns de texto
        if not text or text in {"-", "NONE", "NAN", "NULL", "N/A"}:
            return 0.0
            
        try:
            # 3. Lógica de Detecção de Formato (BR vs US)
            if ',' in text and '.' in text:
                # Caso misto: Assume BR se a vírgula vier DEPOIS do ponto (1.234,56)
                if text.find(',') > text.find('.'):
                    text = text.replace('.', '').replace(',', '.')
                else:
                    # Formato US (1,234.56): Remove vírgula
                    text = text.replace(',', '')
            elif ',' in text:
                # Apenas vírgula (1234,56) -> Troca por ponto
                text = text.replace(',', '.')
            
            # 4. Remove qualquer lixo que sobrou (exceto números, ponto e sinal negativo)
            clean_text = re.sub(r'[^\d.-]', '', text)
            
            return float(clean_text) if clean_text else 0.0
            
        except (ValueError, TypeError):
            return 0.0

    @staticmethod
    def generate_hash(fields: List[Any]) -> str:
        """
        Gera um hash MD5 único para detecção de alterações (Delta Check).
        """
        normalized_fields = []
        
        for f in fields:
            # Tratamento especial para nulos do Pandas e None
            if f is None or pd.isna(f):
                normalized_fields.append("NULL")
            elif isinstance(f, str):
                # Remove espaços duplos e converte para maiúsculo
                clean_str = " ".join(f.strip().upper().split())
                normalized_fields.append(clean_str)
            else:
                # Conversão segura de números e booleanos
                normalized_fields.append(str(f))

        # Cria uma string única separada por pipe "|"
        raw_str = "|".join(normalized_fields)
        
        # Retorna o hash MD5 (Rápido e suficiente para checagem de integridade)
        return hashlib.md5(raw_str.encode('utf-8')).hexdigest()