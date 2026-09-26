"""Vues de l'app core."""

from rest_framework import status
from rest_framework.permissions import AllowAny
from rest_framework.response import Response
from rest_framework.views import APIView

from apps.core import services


class HealthView(APIView):
    """Etat de l'infrastructure. 200 si tout est OK, 503 sinon."""

    authentication_classes: list = []
    permission_classes = [AllowAny]

    def get(self, request):
        result = services.run_health_checks()
        code = (
            status.HTTP_200_OK
            if result["status"] == services.OK
            else status.HTTP_503_SERVICE_UNAVAILABLE
        )
        return Response(result, status=code)
