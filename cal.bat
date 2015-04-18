cls
call gradlew.bat clean
call gradlew.bat
echo Copy ignite-fabric to rapidminer 
copy "..\rapidminer\lib\plugins\rapidminer-ggextension-1.0.0-all.jar" "../libs/ignite-fabric/libs/ext/" /Y

echo Copy to node ignite-fabric 
copy "..\rapidminer\lib\plugins\rapidminer-ggextension-1.0.0-all.jar" "../node/ignite-fabric/libs/ext/" /Y

echo Copy to node rapidminer 
copy "..\rapidminer\lib\plugins\rapidminer-ggextension-1.0.0-all.jar" "../node/rapidminer/lib/plugins/" /Y

call C:\dev\mgr\node\ignite-fabric\bin\ignite.bat


rem copy "..\rapidminer\lib\plugins\rapidminer-ggextension-1.0.0-all.jar" "C:/dev/magisterka/mgr/node/rapidminer/lib/plugins/" /Y
rem copy "..\rapidminer\lib\plugins\rapidminer-ggextension-1.0.0-all.jar" "C:/dev/RapidMiner_Vega/0/lib/plugins/" /Y
rem copy "..\rapidminer\lib\plugins\rapidminer-ggextension-1.0.0-all.jar" "C:/dev/RapidMiner_Vega/1/lib/plugins/" /Y
rem copy "..\rapidminer\lib\plugins\rapidminer-ggextension-1.0.0-all.jar" "C:/dev/RapidMiner_Vega/2/lib/plugins/" /Y
rem copy "..\rapidminer\lib\plugins\rapidminer-ggextension-1.0.0-all.jar" "C:/dev/RapidMiner_Vega/3/lib/plugins/" /Y

