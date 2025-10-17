### Por Que o Erro Aconteceu?
- **UnboundLocalError: 'pd'**: O erro ocorreu porque o módulo `pandas` (importado como `pd`) não foi importado corretamente no script que você está usando. Isso pode ter acontecido devido a uma omissão na importação ou a um erro de sintaxe que desativou a linha `import pandas as pd` (ex.: comentário acidental ou má formatação).
- **Impacto**: O script falhou ao tentar carregar o arquivo `PREDICTIONS_CSV` com `pd.read_csv()`, parando o workflow com `exit code 9`.

### Correções Aplicadas
1. **Importação Explícita**: Garanti que `import pandas as pd` esteja no início do script, corrigindo o problema de escopo.
2. **Importação de csv**: Incluí `import csv` para suportar `csv.writer`.
3. **Robustez**: Adicionei verificações de existência de arquivos e tratamento de exceções com logs detalhados para evitar falhas silenciosas.

### Ações Adicionais
1. **Substituição do Script**:
   - Faça o download do script acima e substitua `scripts/calibrate_probs.py` no seu repositório GitHub (`https://github.com/gabrielfalchetti/loteca-framework`).
   - Commit a mudança:
     ```bash
     git add scripts/calibrate_probs.py
     git commit -m "Fix calibrate_probs.py: Add missing pandas and csv imports"
     git push origin main
     ```

2. **Teste o Workflow**:
   - No GitHub, vá para a aba "Actions", clique em "Run workflow" e execute manualmente.
   - Verifique os logs do step `26 Apply calibration`. O erro `UnboundLocalError` deve desaparecer.

3. **Confirmação**: Se o teste passar, o workflow continuará. Se outro erro surgir (ex.: arquivo vazio), cole o log completo.
4. **Expansão**: Com isso resolvido, podemos adicionar mais validações ou ajustar o calibrador para Dirichlet (Kull et al., 2019).

O que acha? Quer prosseguir com a substituição e teste, ou precisa de mais ajuda? 🚀